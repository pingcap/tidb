// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Go `table.CastValue`: converting a value into its column's type under the
//! caller's type context. Mutation callers complete the raw cast diagnostics.
//!
//! The conversion is shared; the NAMING is not, and that is why this is its
//! own module. Go decorates a failed cast differently at each write site --
//! `completeInsertErr` for an INSERT row, `handleUpdateError` for an UPDATE
//! assignment, and nothing at all for an `ON DUPLICATE KEY UPDATE` one -- so
//! the same value written three ways answers with three different codes and
//! three different messages. [`CastShape`] is that fork, and
//! [`raw_assignment_error`] is the undecorated form the assignment paths
//! report.
//!
//! Mirrors `pkg/table/column.go`'s `castColumnValue`,
//! `pkg/executor/insert_common.go`'s `completeInsertErr` and
//! `pkg/executor/update.go`'s `handleUpdateError`.

use super::*;

/// The source statement caller that completes generated-column errors.
#[derive(Clone, Copy)]
pub(crate) enum GeneratedWrite {
    Insert {
        row_index: usize,
        null_level: crate::bad_null::NullLevel,
    },
    Update {
        row_index: usize,
    },
    OnDuplicate {
        row_index: usize,
        null_level: crate::bad_null::NullLevel,
    },
}

impl GeneratedWrite {
    pub(crate) fn null_level(self, ctx: &crate::StmtContext) -> crate::bad_null::NullLevel {
        match self {
            Self::Insert { null_level, .. } | Self::OnDuplicate { null_level, .. } => null_level,
            Self::Update { .. } => {
                crate::bad_null::NullLevel::from_is_error(ctx.strict() && !ctx.ignore_err())
            }
        }
    }
}

/// Go fillRow/updateRecord: cast each generated value before the next dependency.
/// INSERT also handles each NULL here; UPDATE keeps its later row-wide NULL pass.
/// Storage must not repeat these expressions or their warning side effects.
pub(crate) fn materialize_generated_for_write(
    columns: &[crate::kv_table::KvColumn],
    row: &mut Vec<Datum>,
    ctx: &crate::StmtContext,
    policy: GeneratedWrite,
) -> Result<(), DriverError> {
    row.resize(row.len().max(columns.len()), Datum::Null);
    crate::generated_column::materialize_with(columns, row, false, ctx, |column, value| {
        let mut value = match policy {
            GeneratedWrite::Insert { row_index, .. } => cast_value_for_column(
                value,
                &column.field_type,
                &column.name,
                row_index,
                ctx,
                ctx.ignore_err(),
            ),
            GeneratedWrite::Update { row_index } => cast_value_for_update_assignment(
                value,
                &column.field_type,
                &column.name,
                row_index,
                ctx,
            ),
            GeneratedWrite::OnDuplicate { row_index, .. } => {
                if contextual_cast_supported(&value, &column.field_type) {
                    return cast_value_shaped(
                        value,
                        &column.field_type,
                        &column.name,
                        row_index,
                        ctx,
                        CastShape::GeneratedOnDuplicate,
                        false,
                    );
                }
                // Go updateRecord passes rawVal to the ODKU error handler,
                // whereas ordinary ODKU assignments pass the converted value.
                let source = value.clone();
                cast_value_for_column(
                    value,
                    &column.field_type,
                    &column.name,
                    row_index,
                    ctx,
                    false,
                )
                .map_err(|error| raw_assignment_error(error, &source, &column.field_type))
            }
        }?;
        // fillRow substitutes bad NULL before the next generated expression;
        // updateRecord completes all expressions before its row-wide NULL pass.
        if let GeneratedWrite::Insert { null_level, .. } = policy {
            crate::bad_null::handle_bad_null(
                &mut value,
                &column.field_type,
                &column.name,
                null_level,
                ctx,
            )?;
        }
        Ok(value)
    })
}

impl From<crate::generated_column::GenerationError> for DriverError {
    fn from(error: crate::generated_column::GenerationError) -> Self {
        match error.eval {
            Some(error) => DriverError::Exec(crate::ExecError::Eval(error)),
            None => DriverError::Parse(format!(
                "generated column '{}': {}",
                error.column, error.detail
            )),
        }
    }
}

/// A legacy scalar event that is not itself fatal. Contextual conversions
/// deliver decimal rounding warnings through their own ordered warning sink.
pub(crate) fn conversion_event_is_silent(event: &tidb_datatype::ScalarConversionEvent) -> bool {
    matches!(event, tidb_datatype::ScalarConversionEvent::RoundedToScale)
}

/// Go `table.CastValue` + `completeInsertErr`: converts one written value into
/// the column's own type, and names the failure the way the insert path does.
///
/// The strict SQL mode makes a bad value fail the statement; without it the
/// converted (clamped or truncated) value is stored and the same message is a
/// warning, which is what `sql_mode = ''` produces in TiDB.
///
pub(crate) fn cast_value_for_column(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
    force_ignore_truncate: bool,
) -> Result<Datum, DriverError> {
    cast_value_shaped(
        value,
        field_type,
        column,
        row_index,
        ctx,
        CastShape::InsertRow,
        force_ignore_truncate,
    )
}

/// Go `table.CastValue`: the raw conversion before an INSERT/UPDATE caller
/// completes its error, including the exceptional `forceIgnoreTruncate`
/// switch used by virtual-column and union-scan materialization.
///
/// Reads and union scan retain the caller's type flags and raw error shape;
/// mutation callers complete diagnostics with their own column/row metadata.
pub(crate) fn cast_table_value(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &crate::StmtContext,
    force_ignore_truncate: bool,
) -> Result<Datum, DriverError> {
    cast_table_value_with_flags(
        value,
        field_type,
        column,
        ctx,
        tidb_expr::Columns::type_flags(ctx),
        force_ignore_truncate,
    )
}

/// Go CastColumnValue for a caller with explicit type-context flags, including
/// reorganization contexts whose flags differ from session expression flags.
pub(crate) fn cast_table_value_with_flags(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &crate::StmtContext,
    flags: tidb_datatype::ConversionFlags,
    force_ignore_truncate: bool,
) -> Result<Datum, DriverError> {
    // Go CastColumnValue overrides only matching, on a cloned type. Keep the
    // catalog type and the output datum's declared collation unchanged.
    let legacy_enum_set = matches!(
        field_type.code(),
        tidb_datatype::FieldTypeCode::Enum | tidb_datatype::FieldTypeCode::Set
    ) && !ctx.new_collation_enabled()
        && tidb_datatype::new_collation_enabled();
    let legacy_type;
    let conversion_type = if legacy_enum_set {
        legacy_type = field_type.clone().with_collation_name("binary");
        &legacy_type
    } else {
        field_type
    };
    let mut cast = cast_value_with_flags(
        value,
        conversion_type,
        column,
        0,
        ctx,
        CastShape::RawTable,
        force_ignore_truncate,
        flags,
    )?;
    if legacy_enum_set {
        match &mut cast {
            Datum::Enum(_, collation) | Datum::Set(_, collation) => {
                *collation = field_type.collation();
            }
            _ => {}
        }
    }
    Ok(cast)
}

/// HandleTruncate runs before forceIgnoreTruncate; warnings already appended
/// by the type context survive suppression of the remaining error.
fn handle_raw_cast_error(
    error: DriverError,
    ctx: &crate::StmtContext,
    flags: tidb_datatype::ConversionFlags,
    force_ignore_truncate: bool,
) -> Result<(), DriverError> {
    let reported = error.clone().to_mysql_error();
    let pending = tidb_datatype::TruncationPolicy::new(
        flags.ignore_truncate_err(),
        flags.truncate_as_warning(),
    )
    .handle(
        Some(tidb_error::mysql::SqlError {
            code: reported.code,
            state: tidb_error::mysql::mysql_state(reported.code),
            message: reported.message,
        }),
        |warning| ctx.append_warning_parts(warning.code, &warning.message),
    );
    if pending.is_some() && !force_ignore_truncate {
        Err(error)
    } else {
        Ok(())
    }
}

/// Which source call site names the failure of one cast.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastShape {
    /// `table.CastValue` itself: no statement caller has attached a row.
    RawTable,
    /// `completeInsertErr`: the column and the row are appended, and the code
    /// becomes 1366 / 1265 / 1406 / 1264 accordingly.
    InsertRow,
    /// `handleUpdateError`: `table.CastValue`'s own error, except for
    /// `ErrDataTooLong` and `ErrOverflow`, which keep the decorated form.
    UpdateAssignment,
    /// ODKU preserves raw errors but completes warnings using the converted value.
    OnDuplicateAssignment,
    /// Generated ODKU diagnostics name the original generated value.
    GeneratedOnDuplicate,
}

fn cast_value_shaped(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
    shape: CastShape,
    force_ignore_truncate: bool,
) -> Result<Datum, DriverError> {
    let flags = ctx.write_conversion_flags();
    cast_value_with_flags(
        value,
        field_type,
        column,
        row_index,
        ctx,
        shape,
        force_ignore_truncate,
        flags,
    )
}

#[allow(clippy::too_many_arguments)]
fn cast_value_with_flags(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
    shape: CastShape,
    force_ignore_truncate: bool,
    flags: tidb_datatype::ConversionFlags,
) -> Result<Datum, DriverError> {
    if value.is_null() {
        return Ok(value);
    }
    // Go copies the Datum header, not its string payload. Keep the original
    // for diagnostics without cloning bytes on every generated-column read.
    let source = value;
    let value = &source;
    let incorrect_value = || DriverError::IncorrectValue {
        type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
        value: datum_error_text(&source),
        column: column.to_owned(),
        row: row_index + 1,
    };
    if let Some((converted_bytes, invalid_bytes)) =
        invalid_string_conversion(value, field_type, flags)
    {
        // Go convertToString returns its decoded bytes beside the charset
        // error without applying width production. CastColumnValue names and
        // handles that error before its final CHAR trailing-space pass.
        handle_raw_cast_error(
            DriverError::IncorrectValue {
                type_name: "string".to_owned(),
                value: invalid_bytes
                    .iter()
                    .map(|byte| format!("\\x{byte:02X}"))
                    .collect(),
                column: column.to_owned(),
                row: 0,
            },
            ctx,
            flags,
            force_ignore_truncate,
        )?;
        let converted = if field_type.is_binary_string() {
            Datum::new_bytes(converted_bytes)
        } else {
            Datum::new_collation_string(converted_bytes, field_type.collation())
        };
        return Ok(truncate_char_trailing_spaces(converted, field_type));
    }
    if contextual_cast_supported(&value, field_type) {
        return cast_contextual_value(
            &value,
            &source,
            field_type,
            column,
            row_index,
            ctx,
            shape,
            force_ignore_truncate,
            flags,
        );
    }
    // Go `table.CastValue` passes `sctx.GetSessionVars().StmtCtx.TypeCtx()`,
    // whose location is the session's. A TIMESTAMP column's admissible range
    // is expressed in wall-clock time, so it MOVES with that zone.
    let mut converted = match value.convert_to_in(field_type, flags, &ctx.session_zone()) {
        Ok(converted) => converted,
        // Go's vector conversion errors are plain errors: neither
        // `castColumnValue` nor `completeInsertErr` retitles them as an
        // incorrect/truncated column value.
        Err(error) if field_type.code() == tidb_datatype::FieldTypeCode::VectorFloat32 => {
            return Err(DriverError::Exec(crate::ExecError::Eval(
                tidb_expr::EvalError::Vector(error.to_string()),
            )));
        }
        Err(error) => {
            // go raises a bad TEMPORAL value as `types.ErrWrongValue`
            // (1292) — `Incorrect time value: '...' for column 't' at row
            // 1` — the same text `completeInsertErr` would print for 1366,
            // under the temporal code instead.
            let named = json_write_error(&error).unwrap_or_else(|| {
                if matches!(
                    field_type.code(),
                    tidb_datatype::FieldTypeCode::Timestamp
                        | tidb_datatype::FieldTypeCode::Datetime
                        | tidb_datatype::FieldTypeCode::Date
                        | tidb_datatype::FieldTypeCode::Duration
                ) {
                    return DriverError::IncorrectTemporalValue {
                        type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
                        value: datum_error_text(&source),
                        column: column.to_owned(),
                        row: row_index + 1,
                    };
                }
                incorrect_value()
            });
            return Err(shape.name(named, &source, field_type));
        }
    };
    converted.value = truncate_char_trailing_spaces(converted.value, field_type);
    let Some(event) = converted.event else {
        return Ok(converted.value);
    };
    // go's `ProduceDecWithSpecifiedTp` names EVERY scale rounding through
    // the column scope: `Incorrect decimal value: '<original>' for column
    // '<col>' at row <n>` via HandleTruncate, which a write statement's
    // IgnoreTruncateErr flag downgrades to a warning in every SQL mode
    // (oracle: 99999.99999 -> DECIMAL(10,4) and 1.23456 -> DECIMAL(10,3)
    // both store the rounded value beside the 1366, while a value that fits
    // the scale exactly -- 1.5 -> DECIMAL(10,4) -- stays unremarked).
    let decimal_rounded = matches!(event, tidb_datatype::ScalarConversionEvent::RoundedToScale)
        && field_type.code() == tidb_datatype::FieldTypeCode::NewDecimal;
    if decimal_rounded {
        if shape == CastShape::RawTable {
            let error = DriverError::TruncatedIncorrectValue {
                kind: "DECIMAL".to_owned(),
                value: datum_error_text(&source),
            }
            .to_mysql_error();
            ctx.append_warning_parts(error.code, &error.message);
            return Ok(converted.value);
        }
        ctx.append_warning_parts(
            1366,
            &format!(
                "Incorrect decimal value: '{}' for column '{}' at row {}",
                datum_error_text(&source),
                column,
                row_index + 1,
            ),
        );
        return Ok(converted.value);
    }
    if conversion_event_is_silent(&event) {
        return Ok(converted.value);
    }
    // Go picks the message from the conversion's own error kind: a string
    // that does not fit is ErrDataTooLong, a number outside the column's
    // range is ErrWarnDataOutOfRange, and anything else is the
    // "Incorrect <type> value" form.
    let error = match event {
        tidb_datatype::ScalarConversionEvent::Overflow(_) => DriverError::DataOutOfRange {
            column: column.to_owned(),
            row: row_index + 1,
        },
        // Go `castColumnValue` (`pkg/table/column.go:356`) re-titles a bare
        // `ErrTruncated` as `ErrTruncatedWrongVal` for EVERY column type
        // EXCEPT SET and ENUM, whose conversion is the one that stores the
        // zero value beside the error. Those two therefore keep Go's plain
        // 1265 "Data truncated for column '%s' at row %d".
        tidb_datatype::ScalarConversionEvent::Truncated
            if matches!(
                field_type.code(),
                tidb_datatype::FieldTypeCode::Enum | tidb_datatype::FieldTypeCode::Set
            ) =>
        {
            DriverError::DataTruncatedAtRow {
                column: column.to_owned(),
                row: row_index + 1,
            }
        }
        // A BIT column is the second producer of Go's `ErrDataTooLong`:
        // `convertToMysqlBit` clamps a value wider than the declared `flen`
        // to `(1<<flen)-1` and returns `ErrDataTooLong`, NOT the generic
        // "Incorrect bit value". Captured from TiDB,
        // `INSERT INTO t(a BIT(1)) VALUES (-1)` is 1406
        // "Data too long for column 'a' at row 1", storing `1`.
        tidb_datatype::ScalarConversionEvent::Truncated
            if matches!(field_type.eval_type(), tidb_datatype::EvalType::String)
                || field_type.code() == tidb_datatype::FieldTypeCode::Bit =>
        {
            DriverError::DataTooLong {
                column: column.to_owned(),
                row: row_index + 1,
            }
        }
        // `RoundedToScale` already returned above as silent; it is listed only
        // to keep this match exhaustive.
        tidb_datatype::ScalarConversionEvent::Truncated
        | tidb_datatype::ScalarConversionEvent::RoundedToScale => incorrect_value(),
        tidb_datatype::ScalarConversionEvent::TimestampInDSTTransition => {
            unreachable!("timestamp DST events are handled with the converted temporal value above")
        }
    };
    let error = shape.name(error, &source, field_type);
    if shape == CastShape::RawTable {
        handle_raw_cast_error(error, ctx, flags, force_ignore_truncate)?;
        return Ok(converted.value);
    }
    // Go `ErrCtx.HandleError` (`datum.go:1311` reaches it via
    // `HandleTruncate`): the error survives only when STRICT mode is on AND
    // the statement is not IGNORE — `INSERT IGNORE`/`UPDATE IGNORE` downgrade
    // every write conversion error to a warning and keep the converted
    // (truncated/clamped/zero) value. Go itself notes the blanket shape
    // ("TODO: should not filter all types of errors here"), so the port
    // mirrors it rather than tightening it.
    if ctx.strict() && !ctx.ignore_err() {
        return Err(error);
    }
    let reported = error.to_mysql_error();
    ctx.append_warning_parts(reported.code, &reported.message);
    Ok(converted.value)
}

// Temporal targets share typed diagnostics; table zero-date policy remains here.
// Scalar, temporal and binary sources share numeric/string diagnostics here.
fn contextual_cast_supported(value: &Datum, field: &FieldType) -> bool {
    use tidb_datatype::FieldTypeCode as T;
    if matches!(
        field.code(),
        T::Duration | T::Date | T::Datetime | T::Timestamp
    ) {
        return matches!(
            value,
            Datum::Int(_)
                | Datum::UInt(_)
                | Datum::Real(_)
                | Datum::Float32(_)
                | Datum::Decimal(_)
                | Datum::String(_)
                | Datum::Bytes(_)
                | Datum::Time(_)
                | Datum::Duration(_)
                | Datum::Json(_)
        );
    }

    matches!(
        value,
        Datum::Int(_)
            | Datum::UInt(_)
            | Datum::Real(_)
            | Datum::Float32(_)
            | Datum::Decimal(_)
            | Datum::String(_)
            | Datum::Bytes(_)
            | Datum::Enum(..)
            | Datum::Set(..)
            | Datum::Time(_)
            | Datum::Duration(_)
            | Datum::BinaryLiteral(_)
            | Datum::Bit(_)
            | Datum::Json(_)
    ) && matches!(
        field.code(),
        T::Tiny
            | T::Short
            | T::Int24
            | T::Long
            | T::LongLong
            | T::Float
            | T::Double
            | T::NewDecimal
            | T::String
            | T::Varchar
            | T::VarString
            | T::Blob
            | T::TinyBlob
            | T::MediumBlob
            | T::LongBlob
    )
}

#[allow(clippy::too_many_arguments)]
fn cast_contextual_value(
    value: &Datum,
    source: &Datum,
    field: &FieldType,
    column: &str,
    row: usize,
    ctx: &crate::StmtContext,
    shape: CastShape,
    force_ignore: bool,
    flags: tidb_datatype::ConversionFlags,
) -> Result<Datum, DriverError> {
    use tidb_error::terror::TerrorError;
    #[derive(Default)]
    struct Warnings(std::cell::RefCell<Vec<TerrorError>>);
    impl tidb_datatype::ConversionWarningAppender for Warnings {
        fn append_conversion_warning(&self, warning: TerrorError) {
            self.0.borrow_mut().push(warning);
        }
    }
    let warnings = Warnings::default();
    let zone = ctx.session_zone();
    let context = tidb_datatype::ConversionContext::new(
        flags,
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let result = value.convert_to_in_context(field, &context, &zone);
    let warning_source = match (&result, shape) {
        (Ok(converted), CastShape::OnDuplicateAssignment) => &converted.value,
        _ => source,
    };
    let warning_shape = match shape {
        CastShape::OnDuplicateAssignment | CastShape::GeneratedOnDuplicate => CastShape::InsertRow,
        CastShape::UpdateAssignment => CastShape::RawTable,
        other => other,
    };
    // Conversion stages may emit several warnings before the final error.
    // Preserve their order and let only the INSERT caller complete their text.
    for warning in warnings.0.into_inner() {
        let reported = complete_typed_cast(
            warning.to_sql_error(),
            warning_source,
            field,
            column,
            row,
            warning_shape,
        )
        .to_mysql_error();
        ctx.append_warning_parts(reported.code, &reported.message);
    }
    let converted = result.map_err(|error| {
        json_write_error(&error).unwrap_or_else(|| {
            shape.name(
                DriverError::IncorrectValue {
                    type_name: tidb_datatype::type_str(field.code()).to_owned(),
                    value: datum_error_text(source),
                    column: column.to_owned(),
                    row: row + 1,
                },
                source,
                field,
            )
        })
    })?;
    if let Datum::Time(time) = converted.value {
        let invalid = converted
            .error
            .as_ref()
            .is_some_and(|error| error.to_sql_error().code == 1292);
        if time.is_zero()
            || time.invalid_zero()
            || (field.code() == tidb_datatype::FieldTypeCode::Timestamp && invalid)
        {
            return apply_zero_date(time, invalid, field, source, column, row, ctx, shape);
        }
    }
    if let Some(error) = converted.error {
        let mut raw = error.to_sql_error();
        // castColumnValue retitles only bare ErrTruncated, after ConvertTo.
        if raw.code == 1265 {
            raw.code = 1292;
            raw.message = format!(
                "Truncated incorrect {} value: '{}'",
                field.compact_str(false),
                datum_error_text(source)
            );
        }
        if !flags.ignore_truncate_err() {
            if flags.truncate_as_warning() {
                let reported = complete_typed_cast(
                    raw,
                    if shape == CastShape::OnDuplicateAssignment {
                        &converted.value
                    } else {
                        source
                    },
                    field,
                    column,
                    row,
                    warning_shape,
                )
                .to_mysql_error();
                ctx.append_warning_parts(reported.code, &reported.message);
            } else if !force_ignore {
                if raw.code == 8179
                    && shape == CastShape::InsertRow
                    && field.code() == tidb_datatype::FieldTypeCode::Timestamp
                {
                    return Err(DriverError::IncorrectTemporalValue {
                        type_name: "timestamp".to_owned(),
                        value: datum_error_text(source),
                        column: column.to_owned(),
                        row: row + 1,
                    });
                }
                return Err(complete_typed_cast(raw, source, field, column, row, shape));
            }
        }
    }
    Ok(truncate_char_trailing_spaces(converted.value, field))
}

fn complete_typed_cast(
    error: tidb_error::mysql::SqlError,
    source: &Datum,
    field: &FieldType,
    column: &str,
    row: usize,
    shape: CastShape,
) -> DriverError {
    let insert = shape == CastShape::InsertRow;
    let update = shape == CastShape::UpdateAssignment;
    match error.code {
        1690 if insert || update => DriverError::DataOutOfRange {
            column: column.to_owned(),
            row: row + 1,
        },
        1264 if insert => DriverError::DataOutOfRange {
            column: column.to_owned(),
            row: row + 1,
        },
        1406 if insert || update => DriverError::DataTooLong {
            column: column.to_owned(),
            row: row + 1,
        },
        1265 if insert => DriverError::DataTruncatedAtRow {
            column: column.to_owned(),
            row: row + 1,
        },
        1292 if insert
            && matches!(
                field.code(),
                tidb_datatype::FieldTypeCode::Duration
                    | tidb_datatype::FieldTypeCode::Date
                    | tidb_datatype::FieldTypeCode::Datetime
                    | tidb_datatype::FieldTypeCode::Timestamp
            ) =>
        {
            DriverError::IncorrectTemporalValue {
                type_name: tidb_datatype::type_str(field.code()).to_owned(),
                value: datum_error_text(source),
                column: column.to_owned(),
                row: row + 1,
            }
        }
        1292 if insert => DriverError::IncorrectValue {
            type_name: tidb_datatype::type_str(field.code()).to_owned(),
            value: datum_error_text(source),
            column: column.to_owned(),
            row: row + 1,
        },
        _ => DriverError::Mysql(crate::MysqlError::new(error.code, error.message)),
    }
}

/// Runs the exact charset conversion Go performs before producing a string
/// value and returns both halves only when it found an invalid source group.
fn invalid_string_conversion(
    value: &Datum,
    field_type: &FieldType,
    flags: tidb_datatype::ConversionFlags,
) -> Option<(Vec<u8>, Vec<u8>)> {
    // Binary destinations cannot fail charset conversion. Avoid copying their
    // bytes in this error-only preflight; normal conversion produces them once.
    if field_type.charset() == tidb_datatype::Charset::Binary {
        return None;
    }
    let (bytes, error) = value
        .string_conversion_encoding(field_type, flags)?
        .into_parts();
    error.map(|error| (bytes, error.invalid_bytes().to_vec()))
}

/// Go `table.truncateTrailingSpaces`: a non-binary `CHAR(M)` drops every
/// trailing ASCII space after width handling, including a retained space that
/// fitted inside `M`. Other string families and binary CHAR keep their bytes.
fn truncate_char_trailing_spaces(value: Datum, field_type: &FieldType) -> Datum {
    if field_type.code() != tidb_datatype::FieldTypeCode::String || field_type.is_binary_string() {
        return value;
    }
    let Datum::String(value) = value else {
        return value;
    };
    let collation = value.collation();
    let mut bytes = value.into_bytes();
    bytes.truncate(
        bytes
            .iter()
            .rposition(|byte| *byte != b' ')
            .map_or(0, |i| i + 1),
    );
    Datum::new_collation_string(bytes, collation)
}

/// Go `doDupRowUpdate`'s assignment cast (`pkg/executor/insert.go:495-521`),
/// which differs from the VALUES-row cast above in TWO ways.
///
/// ```text
/// val, err = table.CastValue(sctx, val, c, false, false)
/// if err != nil {
///     return err                       // (1) RAW, not completeInsertErr'd
/// }
/// _ = errorHandler(sctx, assign, &val, nil)   // (2) `val` is the CAST value
/// ```
///
/// (2) is what this fixes: the warnings the cast produced are rewritten with
/// `completeInsertErr(c, val, idxInBatch, ...)` over the ALREADY-CAST value
/// and this row's batch index, so `... ON DUPLICATE KEY UPDATE b = 'abc'`
/// warns `Incorrect int value: '0' for column 'b' at row 1` -- the stored 0,
/// not the source text. The VALUES path calls its handler BEFORE the cast
/// (`InsertValues.handleErr`), which is why the same statement's plain-insert
/// spelling names `'abc'`.
///
/// (1) is [`raw_assignment_error`]: a STRICT assignment returns
/// `table.CastValue`'s error UNWRAPPED -- no column, no row, and a different
/// CODE from the insert spelling of the same value.
/// Go `table.CastValue`'s OWN error, which an assignment returns unchanged.
///
/// The insert path decorates that error with the column and the row
/// (`completeInsertErr`); an `ON DUPLICATE KEY UPDATE` assignment does not
/// (`pkg/executor/insert.go:511-514`, `return err`), and neither does the
/// `UPDATE` path for anything except `ErrDataTooLong` and `ErrOverflow`
/// (`handleUpdateError`). This function names the UNDECORATED error, given
/// the wrapped one this tier built plus the source and target the conversion
/// saw -- the same three inputs Go's producer had.
///
/// Which producer raises which error, from the Go source:
///
/// | conversion | Go producer | message |
/// | --- | --- | --- |
/// | string -> int/uint/float/double/year | `getValidFloatPrefix` (`convert.go:563`) | `Truncated incorrect DOUBLE value: '<s>'` |
/// | string -> decimal, no leading number | `MyDecimal.FromString` (`mydecimal.go:415`) | `Truncated incorrect DECIMAL value: '<s>'` |
/// | string -> decimal, trailing garbage | bare `ErrTruncated`, re-titled by `castColumnValue` with `CompactStr` | `Truncated incorrect decimal(4,1) value: '<s>'` |
/// | string -> time | bare `ErrTruncated`, same re-title | `Truncated incorrect time value: '<s>'` |
/// | string -> date/datetime/timestamp | `ErrWrongValue` | `Incorrect date value: '<s>'` |
/// | too long for a string column | `ProduceStrWithSpecifiedTp` (`datum.go:1302`) | `Data Too Long, field len 3, data len 7` |
/// | unknown ENUM/SET label | bare `ErrTruncated`, NOT re-titled | `Data truncated for column '%s' at row %d` |
///
/// Measured against TiDB for every row, with
/// `insert into k (id) values (1) on duplicate key update <col> = <value>`.
///
/// NOT COVERED, and left carrying the decorated form rather than guessed at:
/// `ErrOverflow` (1264), whose raw spelling an assignment only reaches for a
/// non-constant source (a constant one is refused at build time with 1690),
/// and `BIT`, whose Go form is a THIRD spelling of 1406 with no `data len`
/// (`datum.go:1735`).
/// Go `handleUpdateError` (`pkg/executor/update.go:494`): an `UPDATE`
/// assignment's cast error, which is `table.CastValue`'s own error except for
/// two arms that Go re-titles with the column and the row --
/// `ErrDataTooLong` (through `resetErrDataTooLong`) and `ErrOverflow` (as
/// 1264). Measured:
///
/// ```text
/// update m set i='abc'        [types:1292] Truncated incorrect DOUBLE value: 'abc'
/// update m set v='abcdefg'    [types:1406] Data too long for column 'v' at row 1
/// update m set i=(select 1e30)[types:1264] Out of range value for column 'i' at row 1
/// ```
///
/// DIVERGENCE, measured and left standing: Go answers a DIFFERENT error for
/// the same statement when the planner turns it into a `Point_Get`
/// (`update m set i='abc' where id=1` is `Truncated incorrect INTEGER value`,
/// and the varchar case is the RAW 1406). That is not a second rule about
/// writes -- `buildOrderedList` wraps a point update's assignment in a CAST
/// FUNCTION, and `getValidIntPrefix`'s `isFuncCast` arm names `INTEGER` where
/// the table-cast arm names `DOUBLE`. This tier plans no such specialization,
/// so it answers the general-plan form for both spellings.
pub(crate) fn cast_value_for_update_assignment(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    cast_value_shaped(
        value,
        field_type,
        column,
        row_index,
        ctx,
        CastShape::UpdateAssignment,
        false,
    )
}

impl CastShape {
    /// Names one failed cast, which is the whole difference between the two
    /// shapes. A WARNING carries the same naming as the error would, because
    /// Go builds the object first and only then decides whether the mode
    /// makes it fatal.
    fn name(self, error: DriverError, source: &Datum, field_type: &FieldType) -> DriverError {
        match self {
            Self::RawTable | Self::OnDuplicateAssignment | Self::GeneratedOnDuplicate => {
                raw_assignment_error(error, source, field_type)
            }
            Self::InsertRow => error,
            Self::UpdateAssignment => match error {
                // Go's `handleUpdateError` re-titles exactly these two.
                DriverError::DataTooLong { .. } | DriverError::DataOutOfRange { .. } => error,
                other => raw_assignment_error(other, source, field_type),
            },
        }
    }
}

fn raw_assignment_error(
    wrapped: DriverError,
    source: &Datum,
    field_type: &FieldType,
) -> DriverError {
    use tidb_datatype::{EvalType, FieldTypeCode};

    let source_is_string = matches!(source, Datum::Bytes(_) | Datum::String(_));
    match wrapped {
        // Go's 1366 arm covers the numeric targets, whose conversions raise
        // their own 1292 before `completeInsertErr` renames them.
        DriverError::IncorrectValue { value, .. } if source_is_string => {
            match field_type.eval_type() {
                EvalType::Int | EvalType::Real => DriverError::TruncatedIncorrectValue {
                    kind: "DOUBLE".to_owned(),
                    value,
                },
                // `FromString` fails outright without a leading number and
                // truncates with one; only the first names `DECIMAL`.
                EvalType::Decimal => DriverError::TruncatedIncorrectValue {
                    kind: if starts_with_number(&value) {
                        field_type.compact_str(false)
                    } else {
                        "DECIMAL".to_owned()
                    },
                    value,
                },
                // TIME reaches the same re-title as a truncated decimal.
                EvalType::Duration => DriverError::TruncatedIncorrectValue {
                    kind: field_type.compact_str(false),
                    value,
                },
                _ => DriverError::IncorrectValue {
                    value,
                    type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
                    column: String::new(),
                    row: 0,
                },
            }
        }
        DriverError::IncorrectTemporalValue {
            type_name, value, ..
        } => DriverError::IncorrectValueRaw { type_name, value },
        // A BIT column reports 1406 too, with a Go message this does not
        // model; only the string widths take the raw form.
        DriverError::DataTooLong { .. }
            if field_type.eval_type() == EvalType::String
                && field_type.code() != FieldTypeCode::Bit =>
        {
            DriverError::DataTooLongRaw {
                field_len: field_type.flen().max(0) as u64,
                data_len: datum_error_text(source).chars().count() as u64,
            }
        }
        DriverError::DataTruncatedAtRow { .. } => DriverError::DataTruncatedUnformatted,
        other => other,
    }
}

/// Whether Go's `MyDecimal.FromString` finds a number to read at all: it
/// reports `DECIMAL` when the string has no leading digits after an optional
/// sign, and a plain truncation when it read some and stopped.
fn starts_with_number(text: &str) -> bool {
    let rest = text.trim_start();
    let rest = rest.strip_prefix(['+', '-']).unwrap_or(rest);
    rest.starts_with(|c: char| c.is_ascii_digit() || c == '.')
}

pub(crate) fn cast_value_for_assignment(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
) -> Result<Datum, DriverError> {
    if contextual_cast_supported(&value, field_type) {
        return cast_value_shaped(
            value,
            field_type,
            column,
            row_index,
            ctx,
            CastShape::OnDuplicateAssignment,
            false,
        );
    }
    if value.is_null() {
        return cast_value_for_column(value, field_type, column, row_index, ctx, false);
    }
    if ctx.strict() {
        let source = value.clone();
        return cast_value_for_column(value, field_type, column, row_index, ctx, false)
            .map_err(|wrapped| raw_assignment_error(wrapped, &source, field_type));
    }
    // Non-strict: run the cast with its warning SUPPRESSED, then append the
    // source's own message over the cast value, which is Go's order.
    let before = ctx.warning_count();
    let cast = cast_value_for_column(value, field_type, column, row_index, ctx, false)?;
    ctx.rewrite_warnings_from(before, |code, _message| {
        let reported = match code {
            // `completeInsertErr`'s three arms, over the CAST value.
            1292 => DriverError::IncorrectTemporalValue {
                type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
                value: datum_error_text(&cast),
                column: column.to_owned(),
                row: row_index + 1,
            },
            1366 => DriverError::IncorrectValue {
                type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
                value: datum_error_text(&cast),
                column: column.to_owned(),
                row: row_index + 1,
            },
            // 1264 (out of range) and 1406 (data too long) name no value at
            // all, so re-deriving them would only reproduce what is there.
            _ => return None,
        };
        Some(reported.to_mysql_error().message)
    });
    Ok(cast)
}

/// Go `table.CastValue`'s temporal arm: runs `handleZeroDatetime` over the
/// converted value and turns its verdict into a stored value, a warning plus
/// a stored value, or a statement error.
///
/// `was_invalid` is Go's `tmIsInvalid` -- whether the conversion reported
/// `ErrWrongValue` -- and it is a separate input from "the value is zero"
/// because the two mean different things: a zero that the SOURCE asked for
/// is only a problem under `NO_ZERO_DATE`, while a zero that a FAILED
/// conversion produced is always one.
#[allow(clippy::too_many_arguments)]
fn apply_zero_date(
    converted: tidb_datatype::Time,
    was_invalid: bool,
    field_type: &FieldType,
    source: &Datum,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
    shape: CastShape,
) -> Result<Datum, DriverError> {
    use crate::zero_date::ZeroDateAction;

    let action = crate::zero_date::handle_zero_datetime(
        field_type.code(),
        converted,
        was_invalid,
        ctx.date_modes(),
        ctx.strict(),
    );
    let error = || {
        shape.name(
            DriverError::IncorrectTemporalValue {
                type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
                value: datum_error_text(source),
                column: column.to_owned(),
                row: row_index + 1,
            },
            source,
            field_type,
        )
    };
    match action {
        ZeroDateAction::Store(value) => Ok(value),
        ZeroDateAction::WarnAndStore(value) => {
            let reported = error().to_mysql_error();
            ctx.append_warning_parts(reported.code, &reported.message);
            Ok(value)
        }
        ZeroDateAction::Refuse => Err(error()),
    }
}

/// The `json`-class error a write into a JSON column reports as its own.
///
/// Go's `table.CastValue` returns the error `ParseBinaryJSONFromString`
/// produced unchanged, so a malformed document written into a JSON column is
/// 3140 with the parser's message -- NOT the generic 1366 "Incorrect json
/// value" that every other failed column cast reports. That distinction is
/// SQL-visible: it survives `sql_mode = ''` as an ERROR, because it is the
/// document that cannot exist, not a value that can be clamped.
pub(crate) fn json_write_error(error: &tidb_datatype::DatumValueError) -> Option<DriverError> {
    let json = match error {
        tidb_datatype::DatumValueError::InvalidJsonCharset => tidb_expr::JsonError::InvalidCharset,
        tidb_datatype::DatumValueError::Json(tidb_datatype::BinaryJSONError::EmptyDocument) => {
            tidb_expr::JsonError::EmptyText
        }
        tidb_datatype::DatumValueError::Json(_) => tidb_expr::JsonError::InvalidText,
        _ => return None,
    };
    Some(DriverError::Exec(crate::ExecError::Eval(
        tidb_expr::EvalError::Json(json),
    )))
}

/// A value as MySQL prints it inside a conversion error message.
pub(crate) fn datum_error_text(value: &Datum) -> String {
    match value {
        Datum::Int(v) => v.to_string(),
        Datum::UInt(v) => v.to_string(),
        Datum::Real(v) => v.to_string(),
        Datum::Decimal(v) => v.to_string(),
        Datum::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Datum::String(s) => String::from_utf8_lossy(s.bytes()).into_owned(),
        // Go names a value with `types.Datum.ToString`, which prints a
        // temporal in its own SQL text -- `0000-00-00 00:00:00` for the zero
        // datetime an invalid cast produced, not a debug rendering.
        Datum::Time(time) => time.to_string(),
        Datum::Duration(duration) => duration.to_string(),
        Datum::Json(value) => value.to_string(),
        Datum::BinaryLiteral(value) | Datum::Bit(value) => {
            String::from_utf8_lossy(value.as_bytes()).into_owned()
        }
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use tidb_datatype::{BinaryLiteral, Collation, FieldTypeCode, FieldTypeFlags};

    #[test]
    fn json_result_batch_table_callers_keep_invalid_charset_identity() {
        for shape in [
            CastShape::RawTable,
            CastShape::InsertRow,
            CastShape::UpdateAssignment,
            CastShape::OnDuplicateAssignment,
            CastShape::GeneratedOnDuplicate,
        ] {
            let ctx = crate::StmtContext::for_dml(true, false, false);
            let value = Datum::new_binary_literal(BinaryLiteral::from(vec![0x61]));
            let field = FieldType::new(FieldTypeCode::Json);
            let error = cast_value_shaped(value, &field, "j", 0, &ctx, shape, false)
                .unwrap_err()
                .to_mysql_error();
            assert_eq!(error.code, 3144);
            assert_eq!(
                error.message,
                "Cannot create a JSON value from a string with CHARACTER SET 'binary'."
            );
        }
    }

    #[test]
    fn json_numeric_batch_table_callers_keep_source_warning_and_final_overflow() {
        for shape in [
            CastShape::RawTable,
            CastShape::InsertRow,
            CastShape::UpdateAssignment,
            CastShape::OnDuplicateAssignment,
            CastShape::GeneratedOnDuplicate,
        ] {
            let ctx = crate::StmtContext::for_dml(false, false, false);
            let value = Datum::Json(tidb_datatype::BinaryJSON::parse("\"123.45tail\"").unwrap());
            let field = FieldType::new(FieldTypeCode::NewDecimal)
                .with_flen(3)
                .with_decimal(1);
            let result = cast_value_shaped(value, &field, "a", 4, &ctx, shape, false).unwrap();
            assert_eq!(result.sql_string().unwrap(), "99.9");
            let warnings = ctx.take_warnings();
            assert_eq!(warnings.len(), 2, "{warnings:?}");
            assert_eq!(warnings[0].1, 1265);
            assert_eq!(
                warnings[1].1,
                if matches!(
                    shape,
                    CastShape::InsertRow
                        | CastShape::OnDuplicateAssignment
                        | CastShape::GeneratedOnDuplicate
                ) {
                    1264
                } else {
                    1690
                }
            );
        }
        let ctx = crate::StmtContext::for_dml(false, true, false);
        let error = cast_value_for_column(
            Datum::Json(tidb_datatype::BinaryJSON::parse("{}").unwrap()),
            &FieldType::new(FieldTypeCode::Double),
            "a",
            4,
            &ctx,
            false,
        )
        .unwrap_err()
        .to_mysql_error();
        assert_eq!(error.code, 1366);
        assert_eq!(
            error.message,
            "Incorrect double value: '{}' for column 'a' at row 5"
        );
        let ctx = crate::StmtContext::for_dml(false, true, false);
        let error = cast_table_value(
            Datum::Json(tidb_datatype::BinaryJSON::parse("{}").unwrap()),
            &FieldType::new(FieldTypeCode::Double),
            "a",
            &ctx,
            false,
        )
        .unwrap_err()
        .to_mysql_error();
        assert_eq!(error.code, 1292);
        assert_eq!(error.message, "Truncated incorrect FLOAT value: '{}'");
    }

    #[test]
    fn table_mutation_batch_raw_integer_keeps_overflow_identity() {
        let ctx = crate::StmtContext::for_dml(false, true, false);
        for (input, subject) in [
            ("9223372036854775808", "9223372036854775808"),
            ("1.9tail", "1.9"),
        ] {
            let error = cast_table_value(
                Datum::new_string(input),
                &FieldType::new(FieldTypeCode::LongLong),
                "a",
                &ctx,
                false,
            )
            .unwrap_err()
            .to_mysql_error();
            assert_eq!(error.code, 1690, "{input}");
            assert_eq!(
                error.message,
                format!("BIGINT value is out of range in '{subject}'")
            );
        }
    }

    #[test]
    fn table_mutation_batch_raw_conversion_preserves_warning_order() {
        let ctx = crate::StmtContext::for_query();
        let field = FieldType::new(FieldTypeCode::Tiny);
        let result =
            cast_table_value(Datum::new_string("128tail"), &field, "a", &ctx, false).unwrap();
        assert_eq!(result, Datum::Int(127));
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.len(), 2, "{warnings:?}");
        assert_eq!(
            (warnings[0].1, warnings[0].2.as_str()),
            (1292, "Truncated incorrect DOUBLE value: '128tail'")
        );
        assert_eq!(warnings[1].1, 1690);
    }

    #[test]
    fn table_mutation_batch_varchar_space_truncation_warns() {
        let ctx = crate::StmtContext::for_dml(false, true, false);
        let field = FieldType::new(FieldTypeCode::Varchar).with_flen(2);
        let value = cast_table_value(Datum::new_string("ab  "), &field, "a", &ctx, false).unwrap();
        assert_eq!(datum_error_text(&value), "ab");
        let warnings = ctx.take_warnings();
        assert_eq!(warnings.len(), 1);
        assert_eq!(
            (warnings[0].1, warnings[0].2.as_str()),
            (1265, "Data truncated, field len 2, data len 4")
        );
    }

    #[test]
    fn write_diagnostics_batch_charset_policy_stops_before_width() {
        // Go convertToString skips ProduceStrWithSpecifiedTp after decoding
        // fails; table.CastValue then applies the caller's truncation policy.
        let field = FieldType::new(FieldTypeCode::Varchar)
            .with_flen(1)
            .with_charset_name("ascii")
            .with_collation_name("ascii_bin");
        for shape in [
            CastShape::RawTable,
            CastShape::InsertRow,
            CastShape::UpdateAssignment,
            CastShape::OnDuplicateAssignment,
            CastShape::GeneratedOnDuplicate,
        ] {
            let ctx = crate::StmtContext::for_dml(false, false, false);
            let result = cast_value_shaped(
                Datum::new_string("ab中"),
                &field,
                "a",
                4,
                &ctx,
                shape,
                false,
            )
            .unwrap();
            assert_eq!(datum_error_text(&result), "ab?");
            let warnings = ctx.take_warnings();
            assert_eq!(warnings.len(), 1, "{warnings:?}");
            assert_eq!(warnings[0].1, 1366);
            assert_eq!(
                warnings[0].2,
                "Incorrect string value '\\xE4\\xB8\\xAD' for column 'a'"
            );
        }
        let ctx = crate::StmtContext::for_dml(false, true, false);
        let result = cast_table_value(Datum::new_string("ab中"), &field, "a", &ctx, true).unwrap();
        assert_eq!(datum_error_text(&result), "ab?");
        assert!(ctx.take_warnings().is_empty());
    }

    #[test]
    fn write_diagnostics_batch_binary_and_temporal_keep_typed_errors() {
        let field = FieldType::new(FieldTypeCode::Tiny);
        let ctx = crate::StmtContext::for_dml(false, true, false);
        let values = [
            Datum::new_binary_literal(BinaryLiteral::from(vec![0xff])),
            Datum::Duration(tidb_datatype::MySqlDuration::new(1, 0, 0, 0, 0).unwrap()),
        ];
        for value in values {
            let error = cast_table_value(value, &field, "a", &ctx, false)
                .unwrap_err()
                .to_mysql_error();
            assert_eq!(error.code, 1690, "{}", error.message);
        }
        let wide = Datum::new_binary_literal(BinaryLiteral::from(vec![1; 9]));
        let ctx = crate::StmtContext::for_dml(false, false, false);
        let value = cast_table_value(wide, &field, "a", &ctx, false).unwrap();
        assert_eq!(value, Datum::Int(127));
        let warnings = ctx.take_warnings();
        assert_eq!(
            warnings.iter().map(|w| w.1).collect::<Vec<_>>(),
            vec![1292, 1690]
        );
        assert!(warnings[0]
            .2
            .contains("BINARY value: '0x010101010101010101'"));
    }

    fn assert_strict_cast(
        input: Datum,
        field_type: &FieldType,
        expected: Datum,
        should_fail: bool,
    ) {
        let ctx = crate::StmtContext::for_dml(false, true, false);
        // Go returns its best-effort value beside the error. Rust represents
        // that pair as Converted { value, event }; recover that half before
        // the write layer turns a non-silent event into DriverError.
        let mut converted = input
            .convert_to_in(
                field_type,
                ctx.write_conversion_flags(),
                &ctx.session_zone(),
            )
            .unwrap();
        converted.value = truncate_char_trailing_spaces(converted.value, field_type);
        let conversion_failed = converted
            .event
            .as_ref()
            .is_some_and(|event| !conversion_event_is_silent(event));
        assert_eq!(
            conversion_failed, should_fail,
            "{input:?} -> {field_type:?}"
        );
        assert_eq!(converted.value, expected, "{input:?} -> {field_type:?}");

        let write = cast_value_for_column(input, field_type, "", 0, &ctx, false);
        assert_eq!(write.is_err(), should_fail, "{field_type:?}");
        if !should_fail {
            assert_eq!(write.unwrap(), expected, "{field_type:?}");
        }
    }

    #[test]
    fn test_cast_value() {
        // Direct port of pkg/table/column_test.go::TestCastValue.
        let ctx = crate::StmtContext::for_dml(false, true, false);
        let integer = FieldType::new(FieldTypeCode::Long).with_charset_name("utf8");

        assert_eq!(
            cast_table_value(Datum::Null, &integer, "", &ctx, false).unwrap(),
            Datum::Null
        );

        // Go returns the best-effort zero beside this error.  Rust separates
        // that conversion pair from the write Result, so assert both halves.
        let converted = Datum::new_string("test")
            .convert_to_in(&integer, ctx.write_conversion_flags(), &ctx.session_zone())
            .unwrap();
        assert_eq!(converted.value, Datum::Int(0));
        assert!(converted.event.is_some());
        assert!(cast_table_value(Datum::new_string("test"), &integer, "", &ctx, false).is_err());

        let plain_string = FieldType::new(FieldTypeCode::String);
        assert!(
            cast_table_value(Datum::new_string("test"), &plain_string, "", &ctx, false).is_ok()
        );

        let utf8 = FieldType::new(FieldTypeCode::String).with_charset_name("utf8");
        let mb4_rune = Datum::new_bytes([0xf0, 0x9f, 0x8c, 0x80]);
        assert!(cast_table_value(mb4_rune.clone(), &utf8, "", &ctx, false).is_err());
        assert!(cast_table_value(mb4_rune, &utf8, "", &ctx, true).is_ok());

        let utf8mb4 = FieldType::new(FieldTypeCode::String).with_charset_name("utf8mb4");
        let incomplete_rune = Datum::new_bytes([0xf0, 0x9f, 0x80]);
        assert!(cast_table_value(incomplete_rune.clone(), &utf8mb4, "", &ctx, false).is_err());
        assert!(cast_table_value(incomplete_rune, &utf8mb4, "", &ctx, true).is_ok());

        let ascii = FieldType::new(FieldTypeCode::String).with_charset_name("ascii");
        let non_ascii = Datum::new_bytes([0x32, 0xf0]);
        assert!(cast_table_value(non_ascii.clone(), &ascii, "", &ctx, false).is_err());
        assert!(cast_table_value(non_ascii, &ascii, "", &ctx, true).is_ok());

        let general_ci = FieldType::new(FieldTypeCode::String)
            .with_charset_name("utf8mb4")
            .with_collation_name("utf8mb4_general_ci");
        let good_literal = Datum::new_binary_literal(BinaryLiteral::from(&[0xE5, 0xA5, 0xBD]));
        let cast = cast_table_value(good_literal, &general_ci, "", &ctx, false).unwrap();
        assert_eq!(cast.collation(), Some(Collation::Utf8Mb4GeneralCi));

        for invalid in [
            Datum::new_binary_literal(BinaryLiteral::from(&[0xE5, 0xA5, 0xBD, 0x81])),
            Datum::new_bytes([0xE5, 0xA5, 0xBD, 0x81]),
        ] {
            let error = cast_table_value(invalid, &general_ci, "", &ctx, false)
                .unwrap_err()
                .to_mysql_error();
            assert_eq!(error.code, 1366);
            assert_eq!(
                error.message,
                "Incorrect string value '\\x81' for column ''"
            );
        }
    }

    #[test]
    fn timestamp_dst_gap_keeps_table_warning_and_strict_insert_diagnostic() {
        let field_type = FieldType::new(FieldTypeCode::Timestamp);
        let zone = tidb_datatype::SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        let input = Datum::new_string("2018-03-11 02:00:16");

        let lenient = crate::StmtContext::for_dml(false, false, false).with_time_zone(zone.clone());
        let stored = cast_value_for_column(input.clone(), &field_type, "ts", 0, &lenient, false)
            .expect("non-strict writes store Go's adjusted timestamp");
        assert_eq!(datum_error_text(&stored), "2018-03-11 03:00:00");
        let warnings = lenient.take_warnings();
        assert_eq!(warnings.len(), 1);
        // Lenient table conversion handles the error before INSERT's handleErr.
        // Strict conversion returns it, so INSERT retitles it below.
        assert_eq!(warnings[0].1, 8179);
        assert_eq!(warnings[0].2,
            "Timestamp is not valid, since it is in Daylight Saving Time transition '2018-03-11 02:00:16' for time zone 'America/Los_Angeles'");

        let strict = crate::StmtContext::for_dml(false, true, false).with_time_zone(zone);
        let error = cast_value_for_column(input, &field_type, "ts", 0, &strict, false)
            .expect_err("strict inserts surface Go's completed insert diagnostic");
        let reported = error.to_mysql_error();
        assert_eq!(reported.code, 1292);
        assert!(reported
            .message
            .contains("Incorrect timestamp value: '2018-03-11 02:00:16' for column 'ts' at row 1"));
    }

    #[test]
    fn test_cast_value_strict() {
        // Direct port of pkg/table/column_test.go::TestCastValueStrict: the
        // three failing rows retain the clamped/truncated value, while the
        // three widening or trailing-space rows succeed exactly.
        let unsigned_bigint =
            FieldType::new(FieldTypeCode::LongLong).with_flags(FieldTypeFlags::UNSIGNED);
        assert_strict_cast(Datum::Int(-1), &unsigned_bigint, Datum::UInt(0), true);

        let signed_bigint = FieldType::new(FieldTypeCode::LongLong);
        assert_strict_cast(Datum::Int(1), &signed_bigint, Datum::Int(1), false);

        let signed_int = FieldType::new(FieldTypeCode::Long);
        assert_strict_cast(
            Datum::Int(1_i64 << 40),
            &signed_int,
            Datum::Int(i64::from(i32::MAX)),
            true,
        );
        assert_strict_cast(
            Datum::Int(1_i64 << 16),
            &signed_bigint,
            Datum::Int(1_i64 << 16),
            false,
        );

        let char_two = FieldType::new(FieldTypeCode::String).with_flen(2);
        assert_strict_cast(
            Datum::new_string("abcd"),
            &char_two,
            Datum::new_string("ab"),
            true,
        );
        assert_strict_cast(
            Datum::new_string("a   "),
            &char_two,
            Datum::new_string("a"),
            false,
        );
    }
    #[test]
    fn generated_read_policy_raw_cast_honors_flags_before_force_ignore() {
        let field = FieldType::new(FieldTypeCode::Tiny);
        for (level, force, should_fail, warning_count) in [
            (tidb_expr::ErrorLevel::Error, false, true, 0),
            (tidb_expr::ErrorLevel::Error, true, false, 0),
            (tidb_expr::ErrorLevel::Warn, true, false, 1),
            (tidb_expr::ErrorLevel::Ignore, true, false, 0),
        ] {
            let ctx = crate::StmtContext::for_dml(false, true, false).with_truncate_level(level);
            let value = cast_table_value(Datum::Int(300), &field, "b", &ctx, force);
            assert_eq!(value.is_err(), should_fail, "{level:?}, force={force}");
            if !should_fail {
                assert_eq!(value.unwrap(), Datum::Int(127));
            }
            assert_eq!(ctx.warning_count(), warning_count);
        }
        let ctx = crate::StmtContext::for_query();
        let decimal = FieldType::new(FieldTypeCode::NewDecimal)
            .with_flen(5)
            .with_decimal(1);
        let value = cast_table_value(Datum::new_string("1.25"), &decimal, "b", &ctx, true).unwrap();
        assert_eq!(datum_error_text(&value), "1.3");
        let warnings = ctx.take_warnings();
        assert_eq!(
            (warnings[0].1, warnings[0].2.as_str()),
            (1292, "Truncated incorrect DECIMAL value: '1.25'")
        );
    }
}
