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

//! Go `table.CastValue`: converting ONE written value into its column's type,
//! and naming the failure the way the statement that wrote it does.
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

/// Whether a conversion event is one TiDB reports nothing for.
///
/// Rounding a NUMBER into a narrower decimal is the case: captured, both
/// `INSERT INTO t(d DECIMAL(10,3)) VALUES (1.23456)` and
/// `ALTER TABLE t ADD COLUMN e DECIMAL(6,2) DEFAULT 3.14159` are accepted in
/// silence, storing 1.235 and 3.14. Go reaches that through
/// `ProduceDecWithSpecifiedTp`, whose rounding notice never becomes a
/// statement error. A STRING source is a different case -- it may not be a
/// number at all -- so it is never silent.
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
/// DIVERGENCE, one shape, captured with `gorunmsg` on `t(a BIGINT)`:
/// a string whose numeric prefix is followed by garbage. Under STRICT mode
///
/// ```text
/// insert into t values ('123..34')   TiDB  [types:1264] Out of range value for column 'a' at row 1
///                                    here  [table:1366] Incorrect bigint value: '123..34' for column 'a' at row 1
/// ```
///
/// The stored value and the NON-strict warning both already agree
/// (`123`, and 1366 with that exact text), and so does the read path --
/// `CAST('123..34' AS SIGNED)` is `123` with 1292
/// `Truncated incorrect INTEGER value: '123..34'` on both sides. Only the
/// strict WRITE's error identity differs: Go's `StrToInt` raises
/// `ErrOverflow`, which `completeInsertErr` re-titles as 1264, while the
/// conversion here reports a `ScalarConversionEvent::Truncated` and this
/// function maps that to the "Incorrect <type> value" form.
///
/// Distinguishing them needs `tidb_datatype`'s conversion to carry Go's error
/// IDENTITY beside its event -- the same seam
/// [`cast_value_for_assignment`]'s strict arm needs.
pub(crate) fn cast_value_for_column(
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
        CastShape::InsertRow,
    )
}

/// Which of Go's two namings the failure of one cast takes.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CastShape {
    /// `completeInsertErr`: the column and the row are appended, and the code
    /// becomes 1366 / 1265 / 1406 / 1264 accordingly.
    InsertRow,
    /// `handleUpdateError`: `table.CastValue`'s own error, except for
    /// `ErrDataTooLong` and `ErrOverflow`, which keep the decorated form.
    UpdateAssignment,
}

fn cast_value_shaped(
    value: Datum,
    field_type: &FieldType,
    column: &str,
    row_index: usize,
    ctx: &crate::StmtContext,
    shape: CastShape,
) -> Result<Datum, DriverError> {
    if value.is_null() {
        return Ok(value);
    }
    let incorrect_value = || DriverError::IncorrectValue {
        type_name: tidb_datatype::type_str(field_type.code()).to_owned(),
        value: datum_error_text(&value),
        column: column.to_owned(),
        row: row_index + 1,
    };
    // Go `table.CastValue` passes `sctx.GetSessionVars().StmtCtx.TypeCtx()`,
    // whose location is the session's. A TIMESTAMP column's admissible range
    // is expressed in wall-clock time, so it MOVES with that zone.
    let mut converted = match value.convert_to_in(
        field_type,
        ctx.write_conversion_flags(),
        &ctx.session_zone(),
    ) {
        Ok(converted) => converted,
        // Go returns the value BESIDE the error here, and the temporal seam
        // is the one place the write path needs it: without `NO_ZERO_DATE`
        // and friends a bad date is stored as the zero date with a warning,
        // so an error with no value would have nothing to store.
        Err(tidb_datatype::DatumValueError::IncorrectTemporal(fallback)) => {
            return apply_zero_date(
                fallback, true, field_type, &value, column, row_index, ctx, shape,
            );
        }
        Err(error) => {
            let named = json_write_error(&error).unwrap_or_else(incorrect_value);
            return Err(shape.name(named, &value, field_type));
        }
    };
    converted.value = truncate_char_trailing_spaces(converted.value, field_type);
    if let tidb_datatype::Datum::Time(time) = converted.value {
        return apply_zero_date(
            time, false, field_type, &value, column, row_index, ctx, shape,
        );
    }
    let Some(event) = converted.event else {
        return Ok(converted.value);
    };
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
    };
    let error = shape.name(error, &value, field_type);
    if ctx.strict() {
        return Err(error);
    }
    let reported = error.to_mysql_error();
    ctx.append_warning_parts(reported.code, &reported.message);
    Ok(converted.value)
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
    )
}

impl CastShape {
    /// Names one failed cast, which is the whole difference between the two
    /// shapes. A WARNING carries the same naming as the error would, because
    /// Go builds the object first and only then decides whether the mode
    /// makes it fatal.
    fn name(self, error: DriverError, source: &Datum, field_type: &FieldType) -> DriverError {
        match self {
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
    if value.is_null() {
        return cast_value_for_column(value, field_type, column, row_index, ctx);
    }
    if ctx.strict() {
        let source = value.clone();
        return cast_value_for_column(value, field_type, column, row_index, ctx)
            .map_err(|wrapped| raw_assignment_error(wrapped, &source, field_type));
    }
    // Non-strict: run the cast with its warning SUPPRESSED, then append the
    // source's own message over the cast value, which is Go's order.
    let before = ctx.warning_count();
    let cast = cast_value_for_column(value, field_type, column, row_index, ctx)?;
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
    let tidb_datatype::DatumValueError::Json(error) = error else {
        return None;
    };
    let json = match error {
        tidb_datatype::BinaryJSONError::EmptyDocument => tidb_expr::JsonError::EmptyText,
        _ => tidb_expr::JsonError::InvalidText,
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
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod source_tests {
    use super::*;
    use tidb_datatype::{FieldTypeCode, FieldTypeFlags};

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

        let write = cast_value_for_column(input, field_type, "", 0, &ctx);
        assert_eq!(write.is_err(), should_fail, "{field_type:?}");
        if !should_fail {
            assert_eq!(write.unwrap(), expected, "{field_type:?}");
        }
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
}
