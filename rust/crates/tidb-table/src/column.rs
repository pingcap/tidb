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
use tidb_terror::MysqlError;
use tidb_expr::EvalErrorSql;

/// Go `table.Column`: the table's shared `*model.ColumnInfo`, embedded, and
/// the expression parsed from it once, when the table is loaded.
#[derive(Clone, Debug)]
pub struct Column {
    /// Go's embedded `*model.ColumnInfo`.
    pub info: tidb_model::GoShared<tidb_model::ColumnInfo>,
    /// Go `Column.GeneratedExpr`: `GeneratedExprString` parsed, `None` for
    /// an ordinary column.
    pub generated_expr: Option<tidb_ast::Expr>,
}

impl Column {
    /// Go `ToColumn`: an ordinary column over `info`.
    #[must_use]
    pub fn new(info: tidb_model::GoShared<tidb_model::ColumnInfo>) -> Self {
        Self {
            info,
            generated_expr: None,
        }
    }

    /// The shared `ColumnInfo`, read.
    pub fn read(&self) -> std::sync::RwLockReadGuard<'_, tidb_model::ColumnInfo> {
        self.info.read()
    }
}

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
    Eval(tidb_exprctx::EvalError),
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
    Eval(tidb_exprctx::EvalError),
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
                &crate::table_errors::ERR_COLUMN_CANT_NULL
                    .generate_with_stack_by_args(&[(&column).into()]),
            )),
            ColumnError::NoDefaultForField(field) => Self::Mysql(MysqlError::from_terror(
                &crate::table_errors::ERR_NO_DEFAULT_VALUE
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
    ctx: &dyn tidb_tblctx::MutateContext,
    force_ignore_truncate: bool,
) -> Result<Datum, CastError> {
    cast_table_value_with_flags(
        value,
        field_type,
        column,
        ctx,
        tidb_exprctx::Columns::type_flags(ctx),
        force_ignore_truncate,
    )
}

/// Go CastColumnValue for a caller with explicit type-context flags, including
/// reorganization contexts whose flags differ from session expression flags.
pub fn cast_table_value_with_flags(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    ctx: &dyn tidb_tblctx::MutateContext,
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
    reported: MysqlError,
    ctx: &dyn tidb_tblctx::MutateContext,
    flags: tidb_datatype::ConversionFlags,
    force_ignore_truncate: bool,
) -> Result<(), CastError> {
    let pending = tidb_datatype::TruncationPolicy::new(
        flags.ignore_truncate_err(),
        flags.truncate_as_warning(),
    )
    .handle(
        Some(tidb_mysql::errors::SqlError {
            code: reported.code,
            state: tidb_mysql::errors::mysql_state(reported.code),
            message: reported.message.clone(),
            identity: reported.identity,
        }),
        |warning| {
            ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
                tidb_context::WarningLevel::Warning,
                warning.code,
                warning.message.clone(),
                warning.identity,
            ))
        },
    );
    if pending.is_some() && !force_ignore_truncate {
        Err(CastError::Column(reported))
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
    /// Completes the error a conversion stage returned.
    fn complete_error(
        &self,
        error: tidb_mysql::errors::SqlError,
        source: &Datum,
        field_type: &FieldType,
        column: &str,
        row_index: usize,
    ) -> MysqlError;
    /// Completes a warning a conversion stage raised.
    fn complete_warning(
        &self,
        error: tidb_mysql::errors::SqlError,
        source: &Datum,
        field_type: &FieldType,
        column: &str,
        row_index: usize,
    ) -> MysqlError;
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
    fn complete_error(
        &self,
        error: tidb_mysql::errors::SqlError,
        _: &Datum,
        _: &FieldType,
        _: &str,
        _: usize,
    ) -> MysqlError {
        MysqlError::from_sql_error(error)
    }

    fn complete_warning(
        &self,
        error: tidb_mysql::errors::SqlError,
        _: &Datum,
        _: &FieldType,
        _: &str,
        _: usize,
    ) -> MysqlError {
        MysqlError::from_sql_error(error)
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
    ctx: &dyn tidb_tblctx::MutateContext,
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
        // Go `convertToIncorrectStringErr`.
        let invalid_hex: String = invalid_bytes
            .iter()
            .map(|byte| format!("\\x{byte:02X}"))
            .collect();
        handle_raw_cast_error(
            MysqlError::from_terror(
                &crate::table_errors::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD.fast_generate(
                    "Incorrect string value '%s' for column '%s'",
                    &[invalid_hex.into(), column.into()],
                ),
            ),
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
    ctx: &dyn tidb_tblctx::MutateContext,
    completion: &dyn CastCompletion,
    force_ignore: bool,
    flags: tidb_datatype::ConversionFlags,
) -> Result<Datum, CastError> {
    use tidb_terror::TerrorError;
    #[derive(Default)]
    struct Warnings(std::cell::RefCell<Vec<TerrorError>>);
    impl tidb_context::ConversionWarningAppender for Warnings {
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
        let reported =
            completion.complete_warning(warning.to_sql_error(), warning_source, field, column, row);
        ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            reported.code,
            reported.message.clone(),
            reported.identity,
        ));
    }
    let converted = match result {
        Ok(converted) => converted,
        Err(error) => {
            if field.code() == tidb_datatype::FieldTypeCode::VectorFloat32 {
                return Err(CastError::Eval(tidb_exprctx::EvalError::Vector(
                    error.to_string(),
                )));
            }
            if let Some(eval) = json_write_error(&error) {
                return Err(CastError::Eval(eval));
            }
            // `ConvertTo` failed outright: the producer's error is what the
            // statement caller completes, as for any other conversion error.
            let producer = conversion_failure(source, field).to_sql_error();
            return Err(CastError::Column(
                completion.complete_error(producer, source, field, column, row),
            ));
        }
    };
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
            raw = tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL
                .generate_with_stack_by_args(&[
                    field.compact_str(false).into(),
                    datum_error_text(source).into(),
                ])
                .to_sql_error();
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
                let reported = dst_transition_error(source, column, row);
                ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            reported.code,
            reported.message.clone(),
            reported.identity,
        ));
            } else if flags.truncate_as_warning() {
                let reported = completion.complete_warning(
                    raw,
                    if completion.warns_with_converted_value() {
                        &converted.value
                    } else {
                        source
                    },
                    field,
                    column,
                    row,
                );
                ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            reported.code,
            reported.message.clone(),
            reported.identity,
        ));
            } else if !force_ignore {
                if raw.code == 8179
                    && completion.completes_dst_transition()
                    && field.code() == tidb_datatype::FieldTypeCode::Timestamp
                {
                    return Err(CastError::Column(dst_transition_error(source, column, row)));
                }
                return Err(CastError::Column(
                    completion.complete_error(raw, source, field, column, row),
                ));
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
    if field_type.charset() == tidb_charset::Charset::Binary {
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

/// The error `types.Datum.ConvertTo` raises when a cast fails outright: the
/// producer's own error, before any statement caller completes it with a
/// column and a row (`completeInsertErr`, `handleUpdateError`,
/// `completeLoadErr`) -- `table.CastValue` returns it unchanged to an
/// `ON DUPLICATE KEY UPDATE` assignment and to an `UPDATE`.
///
/// Which producer raises which error, from the Go source:
///
/// | conversion | Go producer | message |
/// | --- | --- | --- |
/// | string -> int/uint/float/double/year | `getValidFloatPrefix` (`convert.go:563`) | `Truncated incorrect DOUBLE value: '<s>'` |
/// | string -> decimal, no leading number | `MyDecimal.FromString` (`mydecimal.go:415`) | `Truncated incorrect DECIMAL value: '<s>'` |
/// | string -> decimal, trailing garbage | bare `ErrTruncated`, re-titled by `castColumnValue` with `CompactStr` | `Truncated incorrect decimal(4,1) value: '<s>'` |
/// | string -> date/datetime/timestamp/time | `ErrWrongValue` | `Incorrect date value: '<s>'` |
///
/// Measured against TiDB for every row, with
/// `insert into k (id) values (1) on duplicate key update <col> = <value>`.
fn conversion_failure(source: &Datum, field_type: &FieldType) -> tidb_terror::TerrorError {
    use tidb_datatype::{EvalType, FieldTypeCode};

    let text = datum_error_text(source);
    let temporal = matches!(
        field_type.code(),
        FieldTypeCode::Timestamp
            | FieldTypeCode::Datetime
            | FieldTypeCode::Date
            | FieldTypeCode::Duration
    );
    let source_is_string = matches!(source, Datum::Bytes(_) | Datum::String(_));
    let truncated = |kind: String| {
        tidb_datatype::types_errors::ERR_TRUNCATED_WRONG_VAL
            .generate_with_stack_by_args(&[kind.into(), text.clone().into()])
    };
    if temporal || !source_is_string {
        return wrong_value(field_type, &text);
    }
    match field_type.eval_type() {
        EvalType::Int | EvalType::Real => truncated("DOUBLE".to_owned()),
        // `FromString` fails outright without a leading number and truncates
        // with one; only the first names `DECIMAL`.
        EvalType::Decimal => truncated(if starts_with_number(&text) {
            field_type.compact_str(false)
        } else {
            "DECIMAL".to_owned()
        }),
        _ => wrong_value(field_type, &text),
    }
}

/// Go `types.ErrWrongValue.GenWithStackByArgs(types.TypeStr(tp), str)`.
fn wrong_value(field_type: &FieldType, text: &str) -> tidb_terror::TerrorError {
    tidb_datatype::types_errors::ERR_WRONG_VALUE.generate_with_stack_by_args(&[
        tidb_datatype::type_str(field_type.code()).into(),
        text.into(),
    ])
}

/// Go `InsertValues.handleErr`'s TIMESTAMP arm for `ErrTimestampInDSTTransition`:
/// `exeerrors.ErrTruncateWrongInsertValue.GenWithStackByArgs(...)`.
fn dst_transition_error(source: &Datum, column: &str, row: usize) -> MysqlError {
    MysqlError::from_terror(
        &tidb_exeerrors::ERR_TRUNCATE_WRONG_INSERT_VALUE
            .generate_with_stack_by_args(&[
                "timestamp".into(),
                datum_error_text(source).into(),
                column.into(),
                (row + 1).into(),
            ]),
    )
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
    ctx: &dyn tidb_tblctx::MutateContext,
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
    let wrong_value_for = |value: &Datum| {
        tidb_datatype::types_errors::ERR_WRONG_VALUE
            .generate_with_stack_by_args(&[
                tidb_datatype::type_str(field_type.code()).into(),
                datum_error_text(value).into(),
            ])
            .to_sql_error()
    };
    match action {
        ZeroDateAction::Store(value) => Ok(value),
        ZeroDateAction::WarnAndStore(value) => {
            // ODKU completes its warnings like the insert row, naming the
            // converted value it stores (`'0000-00-00 00:00:00'`).
            let reported = if completion.warns_with_converted_value() {
                completion.complete_warning(
                    wrong_value_for(&value),
                    &value,
                    field_type,
                    column,
                    row_index,
                )
            } else {
                completion.complete_error(
                    wrong_value_for(source),
                    source,
                    field_type,
                    column,
                    row_index,
                )
            };
            ctx.append_sql_warn(tidb_context::SqlWarn::from_error(
                tidb_context::WarningLevel::Warning,
                reported.code,
                reported.message.clone(),
                reported.identity,
            ));
            Ok(value)
        }
        ZeroDateAction::Refuse => Err(CastError::Column(completion.complete_error(
            wrong_value_for(source),
            source,
            field_type,
            column,
            row_index,
        ))),
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
pub fn json_write_error(error: &tidb_datatype::DatumValueError) -> Option<tidb_exprctx::EvalError> {
    let json = match error {
        tidb_datatype::DatumValueError::InvalidJsonCharset => tidb_exprctx::JsonError::InvalidCharset,
        tidb_datatype::DatumValueError::Json(tidb_datatype::BinaryJSONError::EmptyDocument) => {
            tidb_exprctx::JsonError::EmptyText
        }
        tidb_datatype::DatumValueError::Json(_) => tidb_exprctx::JsonError::InvalidText,
        _ => return None,
    };
    Some(tidb_exprctx::EvalError::Json(json))
}

/// A value as MySQL prints it inside a conversion error message.
pub fn datum_error_text(value: &Datum) -> String {
    // completeInsertErr calls Datum.ToString; keep the shared scalar/enum/set
    // spelling and the existing UTF-8 projection at this protocol boundary.
    String::from_utf8_lossy(&value.sql_bytes().unwrap_or_default()).into_owned()
}
