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

//! Go `getPointGetValue` and `checkCanConvertInPointGet`
//! (`pkg/planner/core/point_get_plan.go`): the constant a point plan looks a
//! key up by, moved into the key column's domain.
//!
//! A point plan replaces the comparison with a KEY LOOKUP, so the constant
//! written in the `WHERE` has to be moved into the COLUMN's domain first --
//! `pk = 1.0` looks up handle `1`, not "no handle at all". Go's rule is one
//! for every column type and every point plan (`PointGet`,
//! `Batch_Point_Get`, handle or unique index):
//!
//!  1. convert the constant to the column's field type, and
//!  2. require the converted value to compare EQUAL to the original.
//!
//! When either step fails the point plan is ABANDONED and the statement falls
//! back to an ordinary scan, whose comparison then decides the rows. A cached
//! fast plan applies the same rule to each execution's parameter
//! (`convertConstant2Datum`), and a parameter that fails it rebuilds no plan.

use std::borrow::Cow;

use tidb_datatype::{Datum, FieldType, FieldTypeCode};
use tidb_exprctx::Columns;

/// Go `checkCanConvertInPointGet`: pairings whose conversion is meaningful
/// for storage but wrong for key equality, so no point plan may be built.
#[must_use]
pub fn can_convert_in_point_get(column: &FieldType, value: &Datum) -> bool {
    if column.eval_type() == tidb_datatype::EvalType::String
        && matches!(
            value,
            Datum::Int(_) | Datum::UInt(_) | Datum::Float32(_) | Datum::Real(_) | Datum::Decimal(_)
        )
    {
        // Column type is String and constant type is numeric.
        return false;
    }
    if column.code() == FieldTypeCode::Bit && matches!(value, Datum::String(_)) {
        // Column type is Bit and constant type is string.
        return false;
    }
    true
}

/// Go `getPointGetValue`'s rule detached from any statement: the constant in
/// the column's domain, or `None` when this statement may not use a point
/// plan at all.
///
/// Conversion runs under strict flags with no warning sink, so a truncated
/// string keeps its best-effort value silently. The fast-plan builders use
/// [`name_value_pair_value`] and [`statement_point_get_value`], which apply
/// the statement's type context as Go does; this one serves callers that
/// reuse the rule outside planning (a cached plan's parameters, join probe
/// keys).
#[must_use]
pub fn point_get_value(column: &FieldType, value: &Datum) -> Option<Datum> {
    if value.is_null() {
        return None;
    }
    if !can_convert_in_point_get(column, value) {
        return None;
    }
    let value = &*in_column_collation(column, value);
    let converted = value
        .convert_to(column, tidb_datatype::STRICT_FLAGS)
        .ok()?
        .value;
    // "The converted result must be same as original datum." A comparison in
    // the ORIGINAL datum's domain, exactly as Go's `dVal.Compare(&d)`: this
    // is what separates `1.0` (equal to `1`, so a point get on handle 1) from
    // `1.5` (not equal to `2`, so no point plan).
    match converted.compare(value, column.collation()) {
        Ok(std::cmp::Ordering::Equal) => Some(converted),
        _ => None,
    }
}

/// "As in buildFromBinOp in util/ranger ... we should set the collation of
/// the string datum to collation of the column": a string or binary-literal
/// constant against a string column is read as a string of the column's
/// collation, so bytes that are not valid in the column's charset (`0xC4E3`
/// against a `gbk` column, read as text) abandon the point plan.
fn in_column_collation<'a>(column: &FieldType, value: &'a Datum) -> Cow<'a, Datum> {
    match value {
        Datum::BinaryLiteral(literal) if column.eval_type() == tidb_datatype::EvalType::String => {
            Cow::Owned(Datum::String(tidb_datatype::StringDatum::new(
                literal.as_bytes().to_vec(),
                column.collation(),
            )))
        }
        Datum::String(text) if column.eval_type() == tidb_datatype::EvalType::String => {
            Cow::Owned(Datum::String(tidb_datatype::StringDatum::new(
                text.bytes().to_vec(),
                column.collation(),
            )))
        }
        other => Cow::Borrowed(other),
    }
}

/// What Go `getNameValuePairs` (`pkg/planner/core/point_get_plan.go`) makes
/// of one `column = constant` pair whose column it found.
#[derive(Clone, Debug, PartialEq)]
pub enum NameValuePairValue {
    /// The constant moved into the column's domain: the pair's key value.
    Key(Datum),
    /// `d.ConvertTo` reported `ErrOverflow`: no stored value can equal the
    /// constant, so Go returns the pair with `isTableDual`.
    TableDual,
    /// `return nil, false`: no point plan for this statement.
    Decline,
}

/// Go `getNameValuePairs`'s conversion of one pair's constant, under the
/// statement's own type context (`evalCtx.TypeCtx()`).
///
/// It differs from [`statement_point_get_value`] exactly where Go's two
/// functions differ: a `CHAR`/`BINARY` column of the binary collation keeps
/// the constant unconverted, `ErrOverflow` is
/// [`NameValuePairValue::TableDual`], and `ErrTruncatedWrongVal` -- what the
/// statement's flags return when truncation is not a warning (a strict
/// `UPDATE`/`DELETE`) -- keeps the best-effort value, while every other
/// conversion error declines.
///
/// Both of Go's calls report through the statement: `ConvertTo` and the
/// round-trip `Compare` each read a string constant as a number, so
/// `id = 'abc'` against an integer key warns `Truncated incorrect DOUBLE
/// value: 'abc'` twice in a `SELECT` (captured), whether or not the point
/// plan is finally used. Where truncation is an error the `Compare` fails
/// and the pair declines, leaving the statement to the ordinary planner.
pub fn name_value_pair_value(
    ctx: &dyn Columns,
    column: &FieldType,
    value: &Datum,
) -> NameValuePairValue {
    if value.is_null() {
        return NameValuePairValue::Decline;
    }
    let value = in_column_collation(column, value);
    if !can_convert_in_point_get(column, &value) {
        return NameValuePairValue::Decline;
    }
    // "This type we needn't to pad `\0` in here": Go keeps the constant as
    // written and converts nothing, so nothing can be reported. The detached
    // rule answers the same key without a diagnostic.
    if column.code() == FieldTypeCode::String
        && column.collation() == tidb_charset::Collation::Binary
    {
        return match point_get_value(column, &value) {
            Some(key) => NameValuePairValue::Key(key),
            None => NameValuePairValue::Decline,
        };
    }
    let warnings = StatementWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let Ok(converted) = value.convert_to_in_context(column, &context, &zone) else {
        return NameValuePairValue::Decline;
    };
    if let Some(error) = &converted.error {
        if tidb_datatype::types_errors::ERR_OVERFLOW.equal(Some(error)) {
            return NameValuePairValue::TableDual;
        }
        // "Some scenarios cast to int with error, but we may use this value
        // in point get."
        if !tidb_parser_types::parser_types_errors::ERR_TRUNCATED_WRONG_VALUE.equal(Some(error)) {
            return NameValuePairValue::Decline;
        }
    }
    if compares_equal(ctx, &converted.value, &value, column, &context, &zone) {
        NameValuePairValue::Key(converted.value)
    } else {
        NameValuePairValue::Decline
    }
}

/// Go `getPointGetValue` under the statement's type context
/// (`stmtCtx.TypeCtx()`): the conversion `newBatchPointGetPlan` applies to
/// every `IN` list element. Any conversion error declines, and so does a
/// value that does not survive the round trip; diagnostics are reported as
/// in [`name_value_pair_value`].
pub fn statement_point_get_value(
    ctx: &dyn Columns,
    column: &FieldType,
    value: &Datum,
) -> Option<Datum> {
    if value.is_null() || !can_convert_in_point_get(column, value) {
        return None;
    }
    let value = in_column_collation(column, value);
    let warnings = StatementWarnings(ctx);
    let zone = ctx.time_zone();
    let context = tidb_datatype::ConversionContext::new(
        ctx.type_flags(),
        tidb_datatype::ConversionLocation::from_time_zone(&zone),
        &warnings,
    );
    let converted = value.convert_to_in_context(column, &context, &zone).ok()?;
    if converted.error.is_some() {
        return None;
    }
    compares_equal(ctx, &converted.value, &value, column, &context, &zone)
        .then_some(converted.value)
}

/// Go's `dVal.Compare(tc, &d, collate.GetCollator(col.GetCollate()))` and its
/// `err != nil || cmp != 0` refusal.
///
/// `compare_with_context` hands the comparison's own truncation -- the string
/// operand read as a number -- back for the caller's policy, which in Go is
/// the `HandleTruncate` inside `StrToFloat`: a warning, nothing, or the error
/// that refuses the pair. Any other comparison error refuses it directly.
fn compares_equal(
    ctx: &dyn Columns,
    converted: &Datum,
    original: &Datum,
    column: &FieldType,
    context: &tidb_datatype::ConversionContext<'_>,
    zone: &tidb_datatype::SessionTimeZone,
) -> bool {
    let (ordering, error) =
        converted.compare_with_context(original, column.collation(), context, zone);
    match error {
        None => {}
        Some(tidb_datatype::DatumValueError::Comparison(message))
            if message.starts_with("Truncated incorrect ") =>
        {
            if tidb_expr::expression_errors::handle_truncate(ctx, &message).is_err() {
                return false;
            }
        }
        Some(_) => return false,
    }
    ordering == std::cmp::Ordering::Equal
}

/// The statement's warning list as the conversion's warning sink.
struct StatementWarnings<'a>(&'a dyn Columns);

impl tidb_context::ConversionWarningAppender for StatementWarnings<'_> {
    fn append_conversion_warning(&self, warning: tidb_terror::TerrorError) {
        let warning = warning.to_sql_error();
        self.0.append_sql_warn(tidb_context::SqlWarn::from_error(
            tidb_context::WarningLevel::Warning,
            warning.code,
            warning.message.clone(),
            warning.identity,
        ));
    }
}
