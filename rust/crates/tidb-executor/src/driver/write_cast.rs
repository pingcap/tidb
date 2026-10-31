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

//! The executor's callers of Go `table.CastValue`: the conversion is
//! `tidb_table::column`'s; this module completes its diagnostics.
//!
//! The conversion is shared; the NAMING is not, and that is why this is its
//! own module. Go decorates a failed cast differently at each write site --
//! `completeInsertErr` for an INSERT row, `handleUpdateError` for an UPDATE
//! assignment, and nothing at all for an `ON DUPLICATE KEY UPDATE` one -- so
//! the same value written three ways answers with three different codes and
//! three different messages. [`CastShape`] is that fork, and
//! the cast's own undecorated error is what the assignment paths
//! report.
//!
//! Mirrors `pkg/executor/insert_common.go`'s `completeInsertErr` /
//! `completeLoadErr` / `handleErr` and `pkg/executor/update.go`'s
//! `handleUpdateError`; [`CastShape`] is the [`CastCompletion`] they make.

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
    table: &tidb_table::kv_table::KvTable,
    row: &mut Vec<Datum>,
    ctx: &crate::StmtContext,
    policy: GeneratedWrite,
    planned: Option<&[Option<Expression>]>,
) -> Result<(), DriverError> {
    let columns = table.columns();
    // A statement row holds the visible columns; generation fills the
    // hidden ones in their `TableInfo.Columns` places.
    if row.len() < columns.len() {
        *row = table.stored_row(std::mem::take(row));
    }
    let convert = |column: &tidb_table::kv_table::KvColumn, value| {
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
            GeneratedWrite::OnDuplicate { row_index, .. } => cast_value_shaped(
                value,
                &column.field_type,
                &column.name,
                row_index,
                ctx,
                CastShape::GeneratedOnDuplicate,
                false,
            ),
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
    };
    let result = match planned {
        // Go `Insert.GenCols`: the planner's own expressions.
        Some(planned) => {
            tidb_table::generated_column::materialize_planned(columns, planned, row, ctx, convert)
        }
        None => {
            tidb_table::generated_column::materialize_with(columns, row, false, ctx, convert)
        }
    };
    result.map_err(|failure| match failure {
        GeneratedWriteFailure::Driver(error) => error,
        GeneratedWriteFailure::Generation(error) => {
            let array_offset = columns
                .iter()
                .position(|column| column.name == error.column)
                .filter(|offset| columns[*offset].field_type.is_array());
            match (policy, array_offset, error.eval) {
                // Go `fillRow`: an array column's evaluation error is
                // completed with its index's name before any truncation
                // handling.
                (GeneratedWrite::Insert { row_index, .. }, Some(offset), Some(eval)) => {
                    DriverError::Exec(crate::ExecError::Eval(complete_func_index_error(
                        table, offset, row_index, eval,
                    )))
                }
                (_, _, eval) => DriverError::from(tidb_table::generated_column::GenerationError {
                    eval,
                    ..error
                }),
            }
        }
    })
}

/// [`materialize_generated_for_write`]'s two failure sources, kept apart so
/// a generation failure can still be completed with its column's index.
enum GeneratedWriteFailure {
    Generation(tidb_table::generated_column::GenerationError),
    Driver(DriverError),
}

impl From<tidb_table::generated_column::GenerationError> for GeneratedWriteFailure {
    fn from(error: tidb_table::generated_column::GenerationError) -> Self {
        Self::Generation(error)
    }
}

impl From<CastError> for GeneratedWriteFailure {
    fn from(error: CastError) -> Self {
        Self::Driver(error.into())
    }
}

impl From<tidb_table::column::ColumnError> for GeneratedWriteFailure {
    fn from(error: tidb_table::column::ColumnError) -> Self {
        Self::Driver(error.into())
    }
}

impl From<DriverError> for GeneratedWriteFailure {
    fn from(error: DriverError) -> Self {
        Self::Driver(error)
    }
}

/// Go `completeError` (`insert_common.go`): names the expression index whose
/// multi-valued column failed -- 3903 for an element of the wrong JSON type,
/// 3752 for an out-of-range number (with the 1-based row), 3907 for a string
/// too long -- and leaves any other error as it was.
fn complete_func_index_error(
    table: &tidb_table::kv_table::KvTable,
    offset: usize,
    row_index: usize,
    error: tidb_exprctx::EvalError,
) -> tidb_exprctx::EvalError {
    use tidb_mysql::errors::FormatArg;
    use tidb_terror::{TerrorClass, TerrorCode, TerrorError};
    let tidb_exprctx::EvalError::Conversion(cause) = &error else {
        return error;
    };
    let name = table
        .func_index_name(offset)
        .unwrap_or("expression_index")
        .to_owned();
    let completed = |code: u16, args: &[FormatArg]| {
        tidb_exprctx::EvalError::Conversion(
            tidb_dbterror::ErrClass(TerrorClass::Expression).new_std(code)
                .generate_with_stack_by_args(args),
        )
    };
    let identity = cause.identity();
    if cause.class() == TerrorClass::Expression
        && cause.code()
            == TerrorCode::new(tidb_errno::errcode::ErrInvalidJSONValueForFuncIndex as isize)
    {
        completed(
            tidb_errno::errcode::ErrInvalidJSONValueForFuncIndex,
            &[FormatArg::from(name)],
        )
    } else if identity == tidb_datatype::types_errors::ERR_OVERFLOW.identity() {
        completed(
            tidb_errno::errcode::ErrDataOutOfRangeFunctionalIndex,
            &[FormatArg::from(name), FormatArg::from(row_index + 1)],
        )
    } else if identity == tidb_datatype::types_errors::ERR_DATA_TOO_LONG.identity() {
        completed(
            tidb_errno::errcode::ErrFunctionalIndexDataIsTooLong,
            &[FormatArg::from(name)],
        )
    } else {
        error
    }
}


use tidb_table::column::*;

// Go executor completion of `table.CastValue` (`pkg/executor/insert_common.go`
// `completeInsertErr` / `completeLoadErr` / `handleErr`,
// `pkg/executor/update.go` `handleUpdateError`, `pkg/executor/insert.go`
// `doDupRowUpdate`): the cast itself is `tidb_table::column`.

/// Go `table.CastValue` + `completeInsertErr`: converts one written value into
/// the column's own type, and names the failure the way the insert path does.
///
/// The strict SQL mode makes a bad value fail the statement; without it the
/// converted (clamped or truncated) value is stored and the same message is a
/// warning, which is what `sql_mode = ''` produces in TiDB.
///
/// [`cast_value_for_column`] for a row an INSERT ... SELECT produced (Go
/// `InsertValues.getRow`), whose cast warnings stay as `CastValue` raised them.
pub fn cast_selected_value_for_column(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &dyn tidb_tblctx::MutateContext,
    force_ignore_truncate: bool,
) -> Result<Datum, CastError> {
    cast_value_shaped(
        value,
        field_type,
        column,
        row_index,
        ctx,
        CastShape::InsertSelectRow,
        force_ignore_truncate,
    )
}

/// [`cast_selected_value_for_column`] for a `LOAD DATA` row
/// ([`CastShape::LoadDataRow`]).
pub fn cast_load_data_value_for_column(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &dyn tidb_tblctx::MutateContext,
    force_ignore_truncate: bool,
) -> Result<Datum, CastError> {
    cast_value_shaped(
        value,
        field_type,
        column,
        row_index,
        ctx,
        CastShape::LoadDataRow,
        force_ignore_truncate,
    )
}

pub fn cast_value_for_column(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &dyn tidb_tblctx::MutateContext,
    force_ignore_truncate: bool,
) -> Result<Datum, CastError> {
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

/// Which source call site names the failure of one cast.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CastShape {
    /// `table.CastValue` itself: no statement caller has attached a row.
    RawTable,
    /// `completeInsertErr`: the column and the row are appended, and the code
    /// becomes 1366 / 1265 / 1406 / 1264 accordingly.
    InsertRow,
    /// `InsertValues.getRow` for INSERT ... SELECT: the error `CastValue`
    /// returns is completed as [`Self::InsertRow`] completes it, but the
    /// warnings it appended stay raw (`constant -1 overflows bigint`), since
    /// only `evalRow` re-completes them.
    InsertSelectRow,
    /// `handleUpdateError`: `table.CastValue`'s own error, except for
    /// `ErrDataTooLong` and `ErrOverflow`, which keep the decorated form.
    UpdateAssignment,
    /// ODKU preserves raw errors but completes warnings using the converted value.
    OnDuplicateAssignment,
    /// Generated ODKU diagnostics name the original generated value.
    GeneratedOnDuplicate,
    /// `InsertValues.getRow` under `LOAD DATA`: warnings stay as `CastValue`
    /// raised them, and an error goes through `completeLoadErr`, which only
    /// retitles `ErrDataTooLong` as `Data truncated for column ... at row N`.
    LoadDataRow,
}

/// Go `table.CastValue` as `shape`'s caller completes it.
pub fn cast_value_shaped(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &dyn tidb_tblctx::MutateContext,
    shape: CastShape,
    force_ignore_truncate: bool,
) -> Result<Datum, CastError> {
    let flags = ctx.write_conversion_flags();
    cast_value_with_flags(
        value,
        field_type,
        column,
        row_index,
        ctx,
        &shape,
        force_ignore_truncate,
        flags,
    )
}

impl CastShape {
    /// Every later decision is the one an INSERT's returned error takes.
    fn error_shape(self) -> Self {
        if self == Self::InsertSelectRow {
            Self::InsertRow
        } else {
            self
        }
    }

    fn warning_shape(self) -> Self {
        match self {
            Self::OnDuplicateAssignment | Self::GeneratedOnDuplicate => Self::InsertRow,
            Self::UpdateAssignment | Self::InsertSelectRow => Self::RawTable,
            // Go's LOAD DATA casts without truncate-as-warning, so every
            // conversion problem reaches `handleErr` as an error and is
            // completed by `completeLoadErr` before it becomes a warning.
            other => other,
        }
    }
}

impl CastCompletion for CastShape {
    fn complete_error(
        &self,
        error: tidb_mysql::errors::SqlError,
        source: &Datum,
        field_type: &FieldType,
        column: &str,
        row_index: usize,
    ) -> MysqlError {
        complete_typed_cast(error, source, field_type, column, row_index, self.error_shape())
    }

    fn complete_warning(
        &self,
        error: tidb_mysql::errors::SqlError,
        source: &Datum,
        field_type: &FieldType,
        column: &str,
        row_index: usize,
    ) -> MysqlError {
        complete_typed_cast(error, source, field_type, column, row_index, self.warning_shape())
    }

    /// ODKU preserves raw errors but completes warnings using the converted
    /// value.
    fn warns_with_converted_value(&self) -> bool {
        *self == Self::OnDuplicateAssignment
    }

    fn completes_dst_transition(&self) -> bool {
        self.error_shape() == Self::InsertRow
    }
}

/// Go `completeInsertErr` (`pkg/executor/insert_common.go:292`),
/// `completeLoadErr` (`:327`) and `handleUpdateError`'s re-titles
/// (`pkg/executor/update.go:494`), chosen by `shape`.
fn complete_typed_cast(
    error: tidb_mysql::errors::SqlError,
    source: &Datum,
    field: &FieldType,
    column: &str,
    row: usize,
    shape: CastShape,
) -> MysqlError {
    let insert = shape == CastShape::InsertRow;
    let update = shape == CastShape::UpdateAssignment;
    let row = row + 1;
    match error.code {
        1690 if insert || update => MysqlError::from_terror(
            &tidb_datatype::types_errors::ERR_WARN_DATA_OUT_OF_RANGE
                .generate_with_stack_by_args(&[column.into(), row.into()]),
        ),
        1264 if insert => MysqlError::from_terror(
            &tidb_datatype::types_errors::ERR_WARN_DATA_OUT_OF_RANGE
                .generate_with_stack_by_args(&[column.into(), row.into()]),
        ),
        1406 if insert || update => MysqlError::from_terror(
            &tidb_datatype::types_errors::ERR_DATA_TOO_LONG
                .generate_with_stack_by_args(&[column.into(), row.into()]),
        ),
        1265 if insert => MysqlError::from_terror(
            &tidb_datatype::types_errors::ERR_TRUNCATED
                .generate_with_stack_by_args(&[column.into(), row.into()]),
        ),
        // Go `completeLoadErr`.
        1406 if shape == CastShape::LoadDataRow => MysqlError::from_terror(
            &tidb_datatype::types_errors::ERR_TRUNCATED
                .generate_with_stack_by_args(&[column.into(), row.into()]),
        ),
        1292 if insert
            && matches!(
                field.code(),
                tidb_datatype::FieldTypeCode::Duration
                    | tidb_datatype::FieldTypeCode::Date
                    | tidb_datatype::FieldTypeCode::Datetime
                    | tidb_datatype::FieldTypeCode::Timestamp
            ) =>
        {
            MysqlError::from_terror(
                &tidb_exeerrors::ERR_TRUNCATE_WRONG_INSERT_VALUE
                    .generate_with_stack_by_args(&[
                        tidb_datatype::type_str(field.code()).into(),
                        datum_error_text(source).into(),
                        column.into(),
                        row.into(),
                    ]),
            )
        }
        1292 if insert => MysqlError::from_terror(
            &tidb_table::table_errors::ERR_TRUNCATED_WRONG_VALUE_FOR_FIELD
                .generate_with_stack_by_args(&[
                    tidb_datatype::type_str(field.code()).into(),
                    datum_error_text(source).into(),
                    column.into(),
                    row.into(),
                ]),
        ),
        _ => MysqlError::from_sql_error(error),
    }
}

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
/// A point UPDATE (`update m set i='abc' where id=1`) answers `Truncated
/// incorrect INTEGER value` and the RAW 1406 instead: that is not a second
/// rule about writes -- Go's `buildOrderedList` wraps the assignment in a CAST
/// FUNCTION before this conversion runs (`point_update_assignment`), and
/// `getValidIntPrefix`'s `isFuncCast` arm names `INTEGER` where the
/// table-cast arm names `DOUBLE`.
pub fn cast_value_for_update_assignment(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &dyn tidb_tblctx::MutateContext,
) -> Result<Datum, CastError> {
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
/// (1) is the cast's own raw error: a STRICT assignment returns
/// `table.CastValue`'s error UNWRAPPED -- no column, no row, and a different
/// CODE from the insert spelling of the same value.
pub fn cast_value_for_assignment(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &dyn tidb_tblctx::MutateContext,
) -> Result<Datum, CastError> {
    cast_value_shaped(
        value,
        field_type,
        column,
        row_index,
        ctx,
        CastShape::OnDuplicateAssignment,
        false,
    )
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use tidb_datatype::{BinaryLiteral, FieldTypeCode, FieldTypeFlags};
use tidb_charset::Collation;

    #[test]
    fn ordinal_owner_batch_table_callers_keep_json_warning_and_ordinal_value() {
        for shape in [
            CastShape::RawTable,
            CastShape::InsertRow,
            CastShape::UpdateAssignment,
            CastShape::OnDuplicateAssignment,
            CastShape::GeneratedOnDuplicate,
        ] {
            for (code, expected) in [
                (FieldTypeCode::Year, "2001"),
                (FieldTypeCode::Enum, "a"),
                (FieldTypeCode::Set, "a"),
            ] {
                let ctx = crate::StmtContext::for_dml(false, false, false);
                let field = FieldType::new(code).with_elems(["a", "b"]);
                let input = Datum::Json(tidb_datatype::BinaryJSON::parse(r#""1tail""#).unwrap());
                let value = cast_value_shaped(input, &field, "a", 4, &ctx, shape, false).unwrap();
                assert_eq!(value.sql_string().unwrap(), expected, "{code:?}");
                let warnings = ctx.take_warnings();
                assert_eq!(warnings.len(), 1, "{warnings:?}");
                if matches!(shape, CastShape::OnDuplicateAssignment) {
                    assert_eq!(
                        warnings[0].message(),
                        format!(
                            "Incorrect {} value: '{expected}' for column 'a' at row 5",
                            tidb_datatype::type_str(code)
                        )
                    );
                }

                assert_eq!(
                    warnings[0].code(),
                    if matches!(
                        shape,
                        CastShape::InsertRow
                            | CastShape::OnDuplicateAssignment
                            | CastShape::GeneratedOnDuplicate
                    ) {
                        1366
                    } else {
                        1292
                    }
                );
            }
        }
    }

    #[test]
    fn ordinal_owner_batch_table_bit_keeps_two_diagnostics() {
        let ctx = crate::StmtContext::for_dml(false, false, false);
        let value = cast_table_value(
            Datum::new_bytes([1; 9]),
            &FieldType::new(FieldTypeCode::Bit).with_flen(8),
            "a",
            &ctx,
            false,
        )
        .unwrap();
        assert_eq!(value, Datum::new_mysql_bit(BinaryLiteral::from(&[255])));
        let warnings = ctx.take_warnings();
        assert_eq!(
            warnings.iter().map(|w| w.code()).collect::<Vec<_>>(),
            [1292, 1406]
        );
    }

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
            assert_eq!(warnings[0].code(), 1265);
            assert_eq!(
                warnings[1].code(),
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
            (warnings[0].code(), warnings[0].message()),
            (1292, "Truncated incorrect DOUBLE value: '128tail'")
        );
        assert_eq!(warnings[1].code(), 1690);
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
            (warnings[0].code(), warnings[0].message()),
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
            assert_eq!(warnings[0].code(), 1366);
            assert_eq!(
                warnings[0].message(),
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
            warnings.iter().map(|w| w.code()).collect::<Vec<_>>(),
            vec![1292, 1690]
        );
        assert!(warnings[0]
            .message()
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
        let conversion_failed = converted.event.as_ref().is_some_and(|event| {
            !matches!(event, tidb_datatype::ScalarConversionEvent::RoundedToScale)
        });
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
        // `HandleTruncate` does not absorb the DST error, so INSERT's
        // `handleErr` retitles it in both modes: a warning here, the error
        // below. Captured from Go TiDB: `Warning 1292 Incorrect timestamp
        // value: '...' for column 'ts' at row 1`.
        assert_eq!(warnings[0].code(), 1292);
        assert_eq!(
            warnings[0].message(),
            "Incorrect timestamp value: '2018-03-11 02:00:16' for column 'ts' at row 1"
        );

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
            (tidb_exprctx::ErrorLevel::Error, false, true, 0),
            (tidb_exprctx::ErrorLevel::Error, true, false, 0),
            (tidb_exprctx::ErrorLevel::Warn, true, false, 1),
            (tidb_exprctx::ErrorLevel::Ignore, true, false, 0),
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
            (warnings[0].code(), warnings[0].message()),
            (1292, "Truncated incorrect DECIMAL value: '1.25'")
        );
    }
}
