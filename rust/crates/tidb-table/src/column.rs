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

//! Go `pkg/table/column.go`'s `CastValue` / `CastColumnValue`: one value
//! converted into a column's type, returning `table.CastValue`'s own error.
//!
//! A statement caller completes that error and the warnings the cast raised
//! with its column and row (Go `completeInsertErr`, `handleUpdateError`,
//! `completeLoadErr`); that completion is the executor's, handed in as a
//! [`CastCompletion`]. `RawCast` is the caller that completes nothing.

use tidb_datatype::{Datum, FieldType};
use tidb_error::mysql::MysqlError;

/// Go `table.CastValue`'s returned `error`: a cast-family `terror` already
/// rendered with its identity, or the evaluation error Go returns unchanged
/// (a malformed JSON document, a VECTOR conversion), kept typed so the
/// statement still sees an evaluation error.
#[derive(Clone, Debug)]
pub enum CastError {
    /// A cast-family error (`ErrTruncatedWrongValueForField`,
    /// `ErrDataTooLong`, `ErrWarnDataOutOfRange`, ...), rendered.
    Column(MysqlError),
    /// An evaluation error the cast returns as it is.
    Eval(tidb_expr::EvalError),
}

impl CastError {
    /// The code, SQLSTATE, message and identity this failure reports.
    #[must_use]
    pub fn to_mysql_error(self) -> MysqlError {
        match self {
            Self::Column(error) => error,
            Self::Eval(error) => error.to_mysql_error(),
        }
    }
}

impl std::fmt::Display for CastError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.clone().to_mysql_error().message)
    }
}



/// Go `pkg/table`'s column helpers' failures (`CheckNotNull`,
/// `GetColDefaultValue`, `FillVirtualColumnValue`, `CastValue`), typed where
/// the executor tells them apart and rendered otherwise.
#[derive(Clone, Debug)]
pub enum ColumnError {
    /// Go `table.ErrColumnCantNull` (1048), naming the column.
    ColumnCannotBeNull(String),
    /// Go `table.ErrNoDefaultValue` (1364), naming the field.
    NoDefaultForField(String),
    /// A Go error already rendered with its identity.
    Mysql(MysqlError),
    /// A typed evaluation failure.
    Eval(tidb_expr::EvalError),
    /// A stored definition this layer cannot read (Go's parser errors).
    Parse(String),
}

/// A column failure raised while reading a stored row (the checksum's
/// origin default): Go's error with its identity, or the row decode
/// failure a stored definition this layer cannot read is.
impl From<ColumnError> for crate::kv_table::KvTableError {
    fn from(error: ColumnError) -> Self {
        match error {
            ColumnError::ColumnCannotBeNull(column) => Self::Mysql(MysqlError::from_terror(
                &tidb_error::catalog::table::ERR_COLUMN_CANT_NULL
                    .generate_with_stack_by_args(&[(&column).into()]),
            )),
            ColumnError::NoDefaultForField(field) => Self::Mysql(MysqlError::from_terror(
                &tidb_error::catalog::table::ERR_NO_DEFAULT_VALUE
                    .generate_with_stack_by_args(&[(&field).into()]),
            )),
            ColumnError::Mysql(error) => Self::Mysql(error),
            ColumnError::Eval(error) => Self::Mysql(error.to_mysql_error()),
            ColumnError::Parse(message) => Self::Decode(message),
        }
    }
}

impl From<CastError> for ColumnError {
    fn from(error: CastError) -> Self {
        match error {
            CastError::Column(error) => Self::Mysql(error),
            CastError::Eval(error) => Self::Eval(error),
        }
    }
}

/// One failed cast before it is rendered: the Go error each producer raises,
/// kept typed while a [`CastCompletion`] re-titles it for its caller.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
pub enum CastFailure {
    IncorrectValue {
        type_name: String,
        value: String,
        column: String,
        row: usize,
    },
    IncorrectTemporalValue {
        type_name: String,
        value: String,
        column: String,
        row: usize,
    },
    DataOutOfRange {
        column: String,
        row: usize,
    },
    DataTooLong {
        column: String,
        row: usize,
    },
    DataTruncatedAtRow {
        column: String,
        row: usize,
    },
    TruncatedIncorrectValue {
        kind: String,
        value: String,
    },
    IncorrectValueRaw {
        type_name: String,
        value: String,
    },
    DataTooLongRaw {
        field_len: u64,
        data_len: u64,
    },
    DataTruncatedUnformatted,
    Mysql(MysqlError),
    Eval(tidb_expr::EvalError),
}

impl CastFailure {
    /// The code, SQLSTATE, message and identity this failure reports.
    #[must_use]
    pub fn to_mysql_error(self) -> MysqlError {
        CastError::from(self).to_mysql_error()
    }
}

impl From<CastFailure> for CastError {
    fn from(failure: CastFailure) -> Self {
        Self::Column(match failure {
            CastFailure::IncorrectValue {
                type_name,
                value,
                column,
                row,
            } => incorrect_value(&type_name, &value, &column, row),
            CastFailure::IncorrectTemporalValue {
                type_name,
                value,
                column,
                row,
            } => MysqlError::from_terror(
                &tidb_util::dbterror::exeerrors::ERR_TRUNCATE_WRONG_INSERT_VALUE
                    .generate_with_stack_by_args(&[
                        (&type_name).into(),
                        (&value).into(),
                        (&column).into(),
                        row.into(),
                    ]),
            ),
            CastFailure::DataOutOfRange { column, row } => MysqlError::from_terror(
                &tidb_error::catalog::types::ERR_WARN_DATA_OUT_OF_RANGE
                    .generate_with_stack_by_args(&[(&column).into(), row.into()]),
            ),
            CastFailure::DataTooLong { column, row } => MysqlError::from_terror(
                &tidb_error::catalog::types::ERR_DATA_TOO_LONG
                    .generate_with_stack_by_args(&[(&column).into(), row.into()]),
            ),
            CastFailure::DataTruncatedAtRow { column, row } => MysqlError::from_terror(
                &tidb_error::catalog::types::ERR_TRUNCATED
                    .generate_with_stack_by_args(&[(&column).into(), row.into()]),
            ),
            CastFailure::TruncatedIncorrectValue { kind, value } => MysqlError::from_terror(
                &tidb_error::catalog::types::ERR_TRUNCATED_WRONG_VAL
                    .generate_with_stack_by_args(&[(&kind).into(), (&value).into()]),
            ),
            CastFailure::IncorrectValueRaw { type_name, value } => MysqlError::from_terror(
                &tidb_error::catalog::types::ERR_WRONG_VALUE
                    .generate_with_stack_by_args(&[(&type_name).into(), (&value).into()]),
            ),
            CastFailure::DataTooLongRaw {
                field_len,
                data_len,
            } => MysqlError::from_terror(
                &tidb_error::catalog::types::ERR_DATA_TOO_LONG.generate_with_stack(format!(
                    "Data Too Long, field len {field_len}, data len {data_len}"
                )),
            ),
            CastFailure::DataTruncatedUnformatted => {
                MysqlError::from_terror(&tidb_error::catalog::types::ERR_TRUNCATED)
            }
            CastFailure::Mysql(error) => error,
            CastFailure::Eval(error) => return Self::Eval(error),
        })
    }
}


/// Go `table.ErrTruncatedWrongValueForField` (1366): "Incorrect %-.32s
/// value: '%-.128s' for column '%.192s' at row %d".
///
/// Go `table.CastValue` converts an invalid character group into the same
/// 1366 error BEFORE an INSERT/UPDATE caller can append a row. Row zero is
/// that raw form; every completed write supplies a one-based row.
#[must_use]
pub fn incorrect_value(type_name: &str, value: &str, column: &str, row: usize) -> MysqlError {
    let message = if row == 0 && type_name == "string" {
        format!("Incorrect string value '{value}' for column '{column}'")
    } else {
        format!("Incorrect {type_name} value: '{value}' for column '{column}' at row {row}")
    };
    MysqlError::from_terror(
        &tidb_error::catalog::table::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD
            .generate_with_stack(message),
    )
}

/// DDL's legacy value/event callers treat scale rounding as nonfatal.
pub fn conversion_event_is_silent(event: &tidb_datatype::ScalarConversionEvent) -> bool {
    matches!(event, tidb_datatype::ScalarConversionEvent::RoundedToScale)
}

/// Go `table.CastValue`: the raw conversion before an INSERT/UPDATE caller
/// completes its error, including the exceptional `forceIgnoreTruncate`
/// switch used by virtual-column and union-scan materialization.
///
/// Reads and union scan retain the caller's type flags and raw error shape;
/// mutation callers complete diagnostics with their own column/row metadata.
pub fn cast_table_value(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &dyn crate::context::MutateContext,
    force_ignore_truncate: bool,
) -> Result<Datum, CastError> {
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
pub fn cast_table_value_with_flags(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &dyn crate::context::MutateContext,
    flags: tidb_datatype::ConversionFlags,
    force_ignore_truncate: bool,
) -> Result<Datum, CastError> {
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
        &RawCast,
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
    error: CastFailure,
    ctx: &dyn crate::context::MutateContext,
    flags: tidb_datatype::ConversionFlags,
    force_ignore_truncate: bool,
) -> Result<(), CastFailure> {
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
            identity: reported.identity,
        }),
        |warning| ctx.append_sql_warn(tidb_util::context::SqlWarn::from_error(
            tidb_util::context::WarningLevel::Warning,
            warning.code,
            warning.message.clone(),
            warning.identity,
        )),
    );
    if pending.is_some() && !force_ignore_truncate {
        Err(error)
    } else {
        Ok(())
    }
}

/// Go's statement caller of `table.CastValue`: how the error the cast returns
/// and the warnings it raised are completed with the caller's column and row
/// (`InsertValues.handleErr` / `completeInsertErr`, `handleUpdateError`,
/// `completeLoadErr`, `doDupRowUpdate`'s handler). The executor owns every
/// completion except `RawCast`.
pub trait CastCompletion {
    /// Names a cast that failed outright, or a zero date the SQL mode
    /// refuses: `failure` is the completed (column, 1-based row) form.
    fn name(&self, failure: CastFailure, source: &Datum, field_type: &FieldType) -> CastFailure;
    /// Completes the error a conversion stage returned.
    fn complete_error(
        &self,
        error: tidb_error::mysql::SqlError,
        source: &Datum,
        field_type: &FieldType,
        column: &str,
        row_index: usize,
    ) -> CastFailure;
    /// Completes a warning a conversion stage raised.
    fn complete_warning(
        &self,
        error: tidb_error::mysql::SqlError,
        source: &Datum,
        field_type: &FieldType,
        column: &str,
        row_index: usize,
    ) -> CastFailure;
    /// Whether warnings name the converted value rather than the source
    /// (`doDupRowUpdate` completes them after the cast).
    fn warns_with_converted_value(&self) -> bool {
        false
    }
    /// Whether `ErrTimestampInDSTTransition` reaches the caller
    /// (`InsertValues.handleErr`'s TIMESTAMP arm).
    fn completes_dst_transition(&self) -> bool {
        false
    }
}

/// `table.CastValue` itself: no statement caller has attached a row, and the
/// error is the cast's own.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RawCast;

impl CastCompletion for RawCast {
    fn name(&self, failure: CastFailure, source: &Datum, field_type: &FieldType) -> CastFailure {
        raw_assignment_error(failure, source, field_type)
    }

    fn complete_error(
        &self,
        error: tidb_error::mysql::SqlError,
        _: &Datum,
        _: &FieldType,
        _: &str,
        _: usize,
    ) -> CastFailure {
        CastFailure::Mysql(MysqlError::from_sql_error(error))
    }

    fn complete_warning(
        &self,
        error: tidb_error::mysql::SqlError,
        _: &Datum,
        _: &FieldType,
        _: &str,
        _: usize,
    ) -> CastFailure {
        CastFailure::Mysql(MysqlError::from_sql_error(error))
    }
}

#[allow(clippy::too_many_arguments)]
/// Go `castColumnValue`, with `completion` completing its diagnostics for
/// the statement caller.
pub fn cast_value_with_flags(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &dyn crate::context::MutateContext,
    completion: &dyn CastCompletion,
    force_ignore_truncate: bool,
    flags: tidb_datatype::ConversionFlags,
) -> Result<Datum, CastError> {
    if value.is_null() {
        return Ok(value);
    }
    // Go copies the Datum header, not its string payload. Keep the original
    // for diagnostics without cloning bytes on every generated-column read.
    let source = value;
    let value = &source;
    if let Some((converted_bytes, invalid_bytes)) =
        invalid_string_conversion(value, field_type, flags)
    {
        // Go convertToString returns its decoded bytes beside the charset
        // error without applying width production. CastColumnValue names and
        // handles that error before its final CHAR trailing-space pass.
        handle_raw_cast_error(
            CastFailure::IncorrectValue {
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
    cast_contextual_value(
        value,
        &source,
        field_type,
        column,
        row_index,
        ctx,
        completion,
        force_ignore_truncate,
        flags,
    )
}

#[allow(clippy::too_many_arguments)]
fn cast_contextual_value(
    value: &Datum,
    source: &Datum,
    field: &FieldType,
    column: &str,
    row: usize,
    ctx: &dyn crate::context::MutateContext,
    completion: &dyn CastCompletion,
    force_ignore: bool,
    flags: tidb_datatype::ConversionFlags,
) -> Result<Datum, CastError> {
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
    let warning_source = match &result {
        Ok(converted) if completion.warns_with_converted_value() => &converted.value,
        _ => source,
    };
    // Conversion stages may emit several warnings before the final error.
    // Preserve their order and let the caller complete their text.
    for warning in warnings.0.into_inner() {
        let reported = completion
            .complete_warning(warning.to_sql_error(), warning_source, field, column, row)
            .to_mysql_error();
        ctx.append_sql_warn(tidb_util::context::SqlWarn::from_error(
            tidb_util::context::WarningLevel::Warning,
            reported.code,
            reported.message.clone(),
            reported.identity,
        ));
    }
    let converted = result.map_err(|error| {
        if field.code() == tidb_datatype::FieldTypeCode::VectorFloat32 {
            return CastFailure::Eval(tidb_expr::EvalError::Vector(error.to_string()));
        }

        json_write_error(&error).map(CastFailure::Eval).unwrap_or_else(|| {
            if matches!(
                field.code(),
                tidb_datatype::FieldTypeCode::Timestamp
                    | tidb_datatype::FieldTypeCode::Datetime
                    | tidb_datatype::FieldTypeCode::Date
                    | tidb_datatype::FieldTypeCode::Duration
            ) {
                return completion.name(
                    CastFailure::IncorrectTemporalValue {
                        type_name: tidb_datatype::type_str(field.code()).to_owned(),
                        value: datum_error_text(source),
                        column: column.to_owned(),
                        row: row + 1,
                    },
                    source,
                    field,
                );
            }

            completion.name(
                CastFailure::IncorrectValue {
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
            return apply_zero_date(time, invalid, field, source, column, row, ctx, completion);
        }
    }
    if let Some(error) = converted.error {
        let mut raw = error.to_sql_error();
        // castColumnValue retitles only bare ErrTruncated, after ConvertTo.
        if raw.code == 1265
            && !matches!(
                field.code(),
                tidb_datatype::FieldTypeCode::Enum | tidb_datatype::FieldTypeCode::Set
            )
        {
            raw.code = 1292;
            raw.message = format!(
                "Truncated incorrect {} value: '{}'",
                field.compact_str(false),
                datum_error_text(source)
            );
        }
        if !flags.ignore_truncate_err() {
            if flags.truncate_as_warning()
                && raw.code == 8179
                && completion.completes_dst_transition()
                && field.code() == tidb_datatype::FieldTypeCode::Timestamp
            {
                // Go `HandleTruncate` does not absorb
                // `ErrTimestampInDSTTransition`, so `InsertValues.handleErr`
                // sees it: a TIMESTAMP column keeps the timestamp already
                // adjusted past the gap and warns `ErrTruncateWrongInsertValue`
                // (1292), the same diagnostic strict mode raises below.
                let reported = CastFailure::IncorrectTemporalValue {
                    type_name: "timestamp".to_owned(),
                    value: datum_error_text(source),
                    column: column.to_owned(),
                    row: row + 1,
                }
                .to_mysql_error();
                ctx.append_sql_warn(tidb_util::context::SqlWarn::from_error(
            tidb_util::context::WarningLevel::Warning,
            reported.code,
            reported.message.clone(),
            reported.identity,
        ));
            } else if flags.truncate_as_warning() {
                let reported = completion
                    .complete_warning(
                        raw,
                        if completion.warns_with_converted_value() {
                            &converted.value
                        } else {
                            source
                        },
                        field,
                        column,
                        row,
                    )
                    .to_mysql_error();
                ctx.append_sql_warn(tidb_util::context::SqlWarn::from_error(
            tidb_util::context::WarningLevel::Warning,
            reported.code,
            reported.message.clone(),
            reported.identity,
        ));
            } else if !force_ignore {
                if raw.code == 8179
                    && completion.completes_dst_transition()
                    && field.code() == tidb_datatype::FieldTypeCode::Timestamp
                {
                    return Err(CastFailure::IncorrectTemporalValue {
                        type_name: "timestamp".to_owned(),
                        value: datum_error_text(source),
                        column: column.to_owned(),
                        row: row + 1,
                    }
                    .into());
                }
                return Err(completion
                    .complete_error(raw, source, field, column, row)
                    .into());
            }
        }
    }
    Ok(truncate_char_trailing_spaces(converted.value, field))
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
pub fn truncate_char_trailing_spaces(value: Datum, field_type: &FieldType) -> Datum {
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
#[must_use]
pub fn raw_assignment_error(
    wrapped: CastFailure,
    source: &Datum,
    field_type: &FieldType,
) -> CastFailure {
    use tidb_datatype::{EvalType, FieldTypeCode};

    let source_is_string = matches!(source, Datum::Bytes(_) | Datum::String(_));
    match wrapped {
        // Go's 1366 arm covers the numeric targets, whose conversions raise
        // their own 1292 before `completeInsertErr` renames them.
        CastFailure::IncorrectValue { value, .. } if source_is_string => {
            match field_type.eval_type() {
                EvalType::Int | EvalType::Real => CastFailure::TruncatedIncorrectValue {
                    kind: "DOUBLE".to_owned(),
                    value,
                },
                // `FromString` fails outright without a leading number and
                // truncates with one; only the first names `DECIMAL`.
                EvalType::Decimal => CastFailure::TruncatedIncorrectValue {
                    kind: if starts_with_number(&value) {
                        field_type.compact_str(false)
                    } else {
                        "DECIMAL".to_owned()
                    },
                    value,
                },
                // TIME reaches the same re-title as a truncated decimal.
                EvalType::Duration => CastFailure::TruncatedIncorrectValue {
                    kind: field_type.compact_str(false),
                    value,
                },
                _ => CastFailure::IncorrectValue {
                    value,
                    type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
                    column: String::new(),
                    row: 0,
                },
            }
        }
        CastFailure::IncorrectTemporalValue {
            type_name, value, ..
        } => CastFailure::IncorrectValueRaw { type_name, value },
        // A BIT column reports 1406 too, with a Go message this does not
        // model; only the string widths take the raw form.
        CastFailure::DataTooLong { .. }
            if field_type.eval_type() == EvalType::String
                && field_type.code() != FieldTypeCode::Bit =>
        {
            CastFailure::DataTooLongRaw {
                field_len: field_type.flen().max(0) as u64,
                data_len: datum_error_text(source).chars().count() as u64,
            }
        }
        CastFailure::DataTruncatedAtRow { .. } => CastFailure::DataTruncatedUnformatted,
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
    ctx: &dyn crate::context::MutateContext,
    completion: &dyn CastCompletion,
) -> Result<Datum, CastError> {
    use crate::zero_date::ZeroDateAction;

    let action = crate::zero_date::handle_zero_datetime(
        field_type.code(),
        converted,
        was_invalid,
        ctx.date_modes(),
        ctx.strict(),
    );
    let error = || {
        completion.name(
            CastFailure::IncorrectTemporalValue {
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
            // ODKU completes its warnings like the insert row, naming the
            // converted value it stores (`'0000-00-00 00:00:00'`).
            let reported = if completion.warns_with_converted_value() {
                CastFailure::IncorrectTemporalValue {
                    type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
                    value: datum_error_text(&value),
                    column: column.to_owned(),
                    row: row_index + 1,
                }
            } else {
                error()
            }
            .to_mysql_error();
            ctx.append_sql_warn(tidb_util::context::SqlWarn::from_error(
            tidb_util::context::WarningLevel::Warning,
            reported.code,
            reported.message.clone(),
            reported.identity,
        ));
            Ok(value)
        }
        ZeroDateAction::Refuse => Err(error().into()),
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
pub fn json_write_error(error: &tidb_datatype::DatumValueError) -> Option<tidb_expr::EvalError> {
    let json = match error {
        tidb_datatype::DatumValueError::InvalidJsonCharset => tidb_expr::JsonError::InvalidCharset,
        tidb_datatype::DatumValueError::Json(tidb_datatype::BinaryJSONError::EmptyDocument) => {
            tidb_expr::JsonError::EmptyText
        }
        tidb_datatype::DatumValueError::Json(_) => tidb_expr::JsonError::InvalidText,
        _ => return None,
    };
    Some(tidb_expr::EvalError::Json(json))
}

/// A value as MySQL prints it inside a conversion error message.
pub fn datum_error_text(value: &Datum) -> String {
    // completeInsertErr calls Datum.ToString; keep the shared scalar/enum/set
    // spelling and the existing UTF-8 projection at this protocol boundary.
    String::from_utf8_lossy(&value.sql_bytes().unwrap_or_default()).into_owned()
}
