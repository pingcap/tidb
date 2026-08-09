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

//! What an omitted column's `DEFAULT` IS, when it is not a settled value.
//!
//! Mirrors Go `pkg/ddl/add_column.go` (`SetDefaultValue`, `getDefaultValue`,
//! `getFuncCallDefaultValue`, `restoreFuncCall`) for the DDL half, and
//! `pkg/table/column.go` (`GetColDefaultValue`, `getColDefaultExprValue`) for
//! what the stored form means on every INSERT.
//!
//! # One mechanism, and where its two spellings come from
//!
//! Go keeps a column default as the PAIR (`ColumnInfo.DefaultValue`,
//! `ColumnInfo.DefaultIsExpr`). Most defaults settle to a value at DDL time.
//! The rest do not: their stored `DefaultValue` is TEXT naming a computation,
//! and every row that omits the column runs that computation afresh. That is
//! the single fact [`ColumnDefault::Computed`] carries.
//!
//! Go splits the computed ones into two only for PRINTING, and this keeps the
//! same split in one boolean rather than in two code paths:
//!
//! * `DEFAULT CURRENT_TIMESTAMP` on a `TIMESTAMP`/`DATETIME` column stores the
//!   marker word and leaves `DefaultIsExpr` FALSE, so
//!   `pkg/executor/show.go`'s `case "CURRENT_TIMESTAMP"` prints it bare, with
//!   the column's own fsp appended when it has one. The fsp the default is
//!   WRITTEN with must already equal the column's, which is why the printer
//!   never has to choose between them -- see [`func_call_default`].
//! * every other computed default sets `DefaultIsExpr` TRUE, and the same
//!   printer's `default:` arm wraps it: `` DEFAULT (`rand()`) `` without the
//!   quotes a literal default would get.
//!
//! # What Go accepts here is a WHITELIST, not "any constant"
//!
//! `getDefaultValue` sends a `DEFAULT` whose expression is a FUNCTION CALL to
//! `getFuncCallDefaultValue`, whose `default:` arm is
//! `ErrDefValGeneratedNamedFunctionIsNotAllowed` (3770). So `DEFAULT (abs(1))`
//! is REFUSED by TiDB even though it folds to a constant, while
//! `DEFAULT (1 + 1)` -- not a function call -- is accepted and stored as `2`.
//! [`build`] therefore routes every function-call default through
//! [`func_call_default`] and never through the constant folder.
//!
//! # Deferred, and refused rather than guessed
//!
//! Go's whitelist also carries `CURRENT_DATE`, `NEXTVAL` (a sequence read),
//! `DATE_FORMAT(NOW(), ...)`, `STR_TO_DATE`, `REPLACE(UPPER(UUID()), ...)`,
//! `UPPER(SUBSTRING_INDEX(USER(), '@', 1))`, the `JSON_*` builders and
//! `VEC_FROM_TEXT`. Each needs its own argument check, and a wrong one would
//! silently store a default TiDB rejects, so the ones not listed in
//! [`func_call_default`] are refused by name.

use tidb_ast::Expr;
use tidb_datatype::{
    ConversionFlags, Converted, Datum, DatumValueError, FieldType, FieldTypeCode, SessionTimeZone,
};
use tidb_expr::expression::Expression;
use tidb_expr::rewriter::{rewrite_expr_resolved, NoResolver};
use tidb_expr::Columns;
use tidb_model::column::COLUMN_INFO_VERSION1;

/// Go's pair (`ColumnInfo.DefaultValue`, `ColumnInfo.DefaultIsExpr`): where
/// the value of an omitted column comes from.
#[derive(Clone, Debug)]
pub enum ColumnDefault {
    /// A value settled at DDL time -- every literal default, and the folded
    /// result of a non-function-call expression such as `DEFAULT (1 + 1)`.
    Value(Datum),
    /// Go's computed default: the stored text names a computation the value
    /// of every omitted column re-runs. Boxed because it is by far the rarer
    /// of the two and several times the size of a settled value, and this
    /// enum sits on every column of every table.
    Computed(Box<ComputedDefault>),
}

/// The body of a [`ColumnDefault::Computed`].
#[derive(Clone, Debug)]
pub struct ComputedDefault {
    /// Go's stored `DefaultValue` string: the marker word
    /// `CURRENT_TIMESTAMP`, or `restoreFuncCall`'s rendering of the call.
    pub text: String,
    /// Go `ColumnInfo.DefaultIsExpr`, which decides only how the default is
    /// PRINTED back -- see this module's docs.
    pub is_expr: bool,
    /// The evaluable form. It reads no column, so it evaluates over an empty
    /// row.
    pub expr: Expression,
}

impl ColumnDefault {
    /// The settled value, for the callers that only make sense for one: the
    /// `ORIGIN_DEFAULT` an `ADD COLUMN` gives to rows written before it, which
    /// Go settles once at DDL time rather than per row.
    #[must_use]
    pub fn settled_value(&self) -> Option<&Datum> {
        match self {
            ColumnDefault::Value(value) => Some(value),
            ColumnDefault::Computed(_) => None,
        }
    }

    /// What follows `DEFAULT ` in `SHOW CREATE TABLE`, quoting included.
    ///
    /// Go `pkg/executor/show.go`: the `CURRENT_TIMESTAMP` marker prints bare
    /// with the column's fsp appended, a `DefaultIsExpr` default prints
    /// parenthesised and unquoted, and a literal prints single-quoted.
    #[must_use]
    pub fn show_create_clause(&self, field_type: &FieldType, literal_text: &str) -> String {
        match self {
            // Go's `if col.GetType() == mysql.TypeBit` arm: a BIT column's
            // default is already a `b'...'` literal and is NOT quoted again.
            ColumnDefault::Value(_) if field_type.code() == FieldTypeCode::Bit => {
                literal_text.to_owned()
            }
            // Go quotes the stored text through `format.OutputFormat`, which
            // doubles a quote and escapes NUL/CR/LF -- without it a default
            // holding any of them prints a body that does not re-parse.
            ColumnDefault::Value(_) => {
                format!("'{}'", tidb_datatype::output_format(literal_text))
            }
            ColumnDefault::Computed(computed) if !computed.is_expr => {
                let fsp = field_type.decimal();
                if fsp > 0 {
                    format!("{}({fsp})", computed.text)
                } else {
                    computed.text.clone()
                }
            }
            ColumnDefault::Computed(computed) => format!("({})", computed.text),
        }
    }

    /// The `Default` cell of `SHOW COLUMNS` / `DESCRIBE`.
    ///
    /// Go `pkg/table/column.go`'s `NewColDesc` reports the STORED string, and
    /// appends the fsp only to the `CURRENT_TIMESTAMP` marker on a
    /// `TIMESTAMP`/`DATETIME` column -- so an expression default reports its
    /// text with no parentheses, unlike `SHOW CREATE TABLE`.
    #[must_use]
    pub fn column_desc_text(&self, field_type: &FieldType) -> Option<String> {
        match self {
            ColumnDefault::Value(_) => None,
            ColumnDefault::Computed(computed) if !computed.is_expr => {
                let fsp = field_type.decimal();
                let temporal = matches!(
                    field_type.code(),
                    FieldTypeCode::Timestamp | FieldTypeCode::Datetime
                );
                Some(if temporal && fsp > 0 {
                    format!("{}({fsp})", computed.text)
                } else {
                    computed.text.clone()
                })
            }
            ColumnDefault::Computed(computed) => Some(computed.text.clone()),
        }
    }

    /// Go `NewColDesc`'s `Extra` for a column whose default is an expression.
    #[must_use]
    pub fn is_default_generated(&self) -> bool {
        matches!(self, ColumnDefault::Computed(computed) if computed.is_expr)
    }
}

/// Why a `DEFAULT` was refused at DDL time.
#[derive(Clone, Debug)]
pub enum DefaultError {
    /// Go `ErrDefValGeneratedNamedFunctionIsNotAllowed` (3770): the default
    /// names a function TiDB does not allow there. The payload is the
    /// function name as Go reports it.
    FunctionNotAllowed(String),
    /// Go `dbterror.ErrInvalidDefaultValue` (1067): the default is not one
    /// this column can carry. The caller names the column.
    InvalidDefault,
    /// A form Go accepts that this tier does not model yet, named so a
    /// refusal says which.
    Unsupported(&'static str),
}

impl DefaultError {
    /// Attaches the column name and converts this shared DEFAULT decision to
    /// the exact driver error every DDL frontend must expose.
    pub fn into_driver_error(self, column: &str) -> crate::DriverError {
        match self {
            Self::FunctionNotAllowed(function) => {
                crate::DriverError::DefaultFunctionNotAllowed(column.to_owned(), function)
            }
            Self::InvalidDefault => crate::DriverError::InvalidDefault(column.to_owned()),
            Self::Unsupported(reason) => crate::DriverError::unsupported(reason),
        }
    }
}

/// The names Go's `getFuncCallDefaultValue` treats as the clock marker on a
/// temporal column: `CURRENT_TIMESTAMP` and its aliases, which the parser
/// normalises to the same node Go's `ast.CurrentTimestamp` names.
fn is_clock_marker(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "current_timestamp" | "now" | "localtime" | "localtimestamp"
    )
}

/// Whether an expression is a bare `CURRENT_TIMESTAMP` in any spelling the
/// parser produces: the keyword may arrive as a zero-argument call or as a
/// bare identifier.
fn clock_marker_call(expr: &Expr) -> Option<&[Expr]> {
    match expr {
        Expr::Func { name, args, .. } if is_clock_marker(name) => Some(args),
        Expr::Column(path) => match path.as_slice() {
            [only] if is_clock_marker(only) => Some(&[]),
            _ => None,
        },
        _ => None,
    }
}

/// The fsp an explicit `CURRENT_TIMESTAMP(n)` argument names.
///
/// Go reads it as `expr.Args[0].(*driver.ValueExpr).GetInt64()`, so a
/// non-integer there is not a value it has a reading for; `None` folds to the
/// same 0 Go's zero-valued `ValueExpr` would give, which a column with an fsp
/// then rejects. The parser admits only an integer literal in this position,
/// so `None` is unreachable for a statement that parsed.
fn clock_fsp_argument(arg: &Expr) -> Option<i64> {
    match arg {
        Expr::Int(digits) => digits.parse().ok(),
        _ => None,
    }
}

/// Go `restoreFuncCall`'s flag set, which is what `ColumnInfo.DefaultValue`
/// stores for an expression default and therefore what `SHOW CREATE TABLE`
/// prints back.
fn default_restore_flags() -> tidb_ast::RestoreFlags {
    tidb_ast::RestoreFlags::STRING_SINGLE_QUOTES
        | tidb_ast::RestoreFlags::KEYWORD_LOWERCASE
        | tidb_ast::RestoreFlags::NAME_BACK_QUOTES
        | tidb_ast::RestoreFlags::SPACES_AROUND_BINARY_OPERATION
}

/// Go `getFuncCallDefaultValue`: the whitelist a function-call `DEFAULT` must
/// be on, and what it stores.
///
/// `Ok(None)` is Go's `(nil, false, nil)` return, which means "not settled
/// here, keep going" -- the clock marker on a column that is not temporal.
fn func_call_default(
    name: &str,
    args: &[Expr],
    expr: &Expr,
    field_type: &FieldType,
) -> Result<Option<ColumnDefault>, DefaultError> {
    let lower = name.to_ascii_lowercase();
    if is_clock_marker(&lower) {
        // Go: the marker settles only on a temporal column; anywhere else it
        // falls through to `EvalSimpleAst`, which FREEZES the clock reading
        // into a literal. That frozen form is not modelled -- see below.
        if !matches!(
            field_type.code(),
            FieldTypeCode::Timestamp | FieldTypeCode::Datetime
        ) {
            return Ok(None);
        }
        // Go `getFuncCallDefaultValue`: the fsp WRITTEN on the default -- 0
        // when it is written bare -- must EQUAL the column's own, so
        // `DATETIME(3)` demands `CURRENT_TIMESTAMP(3)` and refuses the bare
        // word just as firmly as it refuses `CURRENT_TIMESTAMP(2)`. Go reads
        // the argument only when there is exactly one; any other arity leaves
        // the written fsp at 0, which a column with an fsp then rejects.
        let written_fsp = match args {
            [only] => clock_fsp_argument(only).unwrap_or(0),
            _ => 0,
        };
        if written_fsp != field_type.decimal() {
            return Err(DefaultError::InvalidDefault);
        }
        return Ok(Some(ColumnDefault::Computed(Box::new(ComputedDefault {
            // Go stores the marker word itself, not the written spelling, so
            // `DEFAULT now()` and `DEFAULT current_timestamp` are one table.
            text: "CURRENT_TIMESTAMP".to_owned(),
            is_expr: false,
            expr: build_expression(expr)?,
        }))));
    }
    match lower.as_str() {
        // Go's no-argument-check arms: `RAND()`, `UUID()`. `VerifyArgsWrapper`
        // is the builder's own arity check here, which the rewriter performs.
        "rand" | "uuid" => Ok(Some(ColumnDefault::Computed(Box::new(ComputedDefault {
            text: expr.restore_with_flags(default_restore_flags()),
            is_expr: true,
            expr: build_expression(expr)?,
        })))),
        _ => Err(DefaultError::FunctionNotAllowed(lower)),
    }
}

/// Builds the evaluable form of a computed default, refusing rather than
/// storing a default whose expression this tier cannot evaluate.
fn build_expression(expr: &Expr) -> Result<Expression, DefaultError> {
    rewrite_expr_resolved(expr, &NoResolver)
        .map_err(|_| DefaultError::Unsupported("a DEFAULT expression this node cannot evaluate"))
}

/// Go `SetDefaultValue` -> `getDefaultValue` for one written `DEFAULT`.
///
/// `fold` settles the non-function-call forms, which is the caller's existing
/// constant path (it owns the per-type normalisation Go's `getDefaultValue`
/// tail performs). This decides only WHICH of the two worlds the default is
/// in, so the whitelist above is the single place that answers it.
pub fn build(
    expr: &Expr,
    field_type: &FieldType,
    fold: impl FnOnce(&Expr) -> Result<Datum, DefaultError>,
) -> Result<ColumnDefault, DefaultError> {
    let call = match expr {
        Expr::Func { name, args, .. } => Some((name.clone(), args.as_slice())),
        // A bare `CURRENT_TIMESTAMP` keyword reaches the builder as an
        // identifier, and Go's parser hands `getDefaultValue` the same
        // function node either way.
        _ => clock_marker_call(expr).map(|args| ("CURRENT_TIMESTAMP".to_owned(), args)),
    };
    if let Some((name, args)) = call {
        if let Some(default) = func_call_default(&name, args, expr, field_type)? {
            return Ok(default);
        }
        // Go falls through to `EvalSimpleAst` here, freezing the clock into a
        // literal on a non-temporal column. Storing a frozen timestamp is a
        // silently WRONG table rather than a missing feature, so it is
        // refused by name.
        return Err(DefaultError::Unsupported(
            "a DEFAULT CURRENT_TIMESTAMP on a column that is not TIMESTAMP or DATETIME",
        ));
    }
    fold(expr).map(ColumnDefault::Value)
}

/// Materializes one literal `ColumnInfo.DefaultValue` in the consumer's zone.
///
/// Go `pkg/table/column.go::getColDefaultValue` casts every non-`TIMESTAMP`
/// value with the consumer context. A literal `TIMESTAMP` has a second zone
/// authority: version-1-and-later metadata stores its wall clock in UTC,
/// while version 0 predates that contract and has already lost its original
/// zone, so Go reads it in `timeutil.SystemLocation()`. After parsing in that
/// source zone, Go projects every nonzero timestamp into the session zone.
///
/// The conversion event belongs to the original cast. Reprojection changes
/// only the produced wall clock and deliberately leaves that event intact.
pub fn materialize_stored_literal(
    stored: &Datum,
    field_type: &FieldType,
    column_info_version: u64,
    flags: ConversionFlags,
    session_zone: &SessionTimeZone,
) -> Result<Converted<Datum>, DatumValueError> {
    materialize_stored_literal_with_system_zone(
        stored,
        field_type,
        column_info_version,
        flags,
        session_zone,
        system_location_as_session_zone,
    )
}

fn system_location_as_session_zone() -> SessionTimeZone {
    match tidb_util::timeutil::system_location() {
        tidb_util::timeutil::TimeZone::Local => SessionTimeZone::Local,
        tidb_util::timeutil::TimeZone::Named(zone) => SessionTimeZone::Named(zone),
        tidb_util::timeutil::TimeZone::Fixed { name, offset_secs } => {
            SessionTimeZone::Fixed { name, offset_secs }
        }
    }
}

fn materialize_stored_literal_with_system_zone(
    stored: &Datum,
    field_type: &FieldType,
    column_info_version: u64,
    flags: ConversionFlags,
    session_zone: &SessionTimeZone,
    system_zone: impl FnOnce() -> SessionTimeZone,
) -> Result<Converted<Datum>, DatumValueError> {
    if field_type.code() != FieldTypeCode::Timestamp {
        return stored.convert_to_in(field_type, flags, session_zone);
    }

    let utc = SessionTimeZone::utc();
    let resolved_system_zone;
    let source_zone = if column_info_version >= COLUMN_INFO_VERSION1 {
        &utc
    } else {
        resolved_system_zone = system_zone();
        &resolved_system_zone
    };
    let mut converted = stored.convert_to_in(field_type, flags, source_zone)?;
    let Datum::Time(time) = &mut converted.value else {
        return Ok(converted);
    };
    if time.is_zero() {
        return Ok(converted);
    }
    time.convert_time_zone(source_zone, session_zone)
        .map_err(|error| DatumValueError::Comparison(error.to_string()))?;
    Ok(converted)
}

/// Go `table.GetColDefaultValue`: the value an omitted column takes for ONE
/// row. A settled default is that value; a computed one is evaluated now,
/// against the statement's own context, so `CURRENT_TIMESTAMP` reads the
/// statement clock exactly as Go's `getColDefaultExprValue` does.
pub fn evaluate(
    default: &ColumnDefault,
    field_type: &FieldType,
    column_info_version: u64,
    flags: ConversionFlags,
    ctx: &impl Columns,
    row: tidb_chunk::row::Row<'_>,
) -> Result<Datum, tidb_expr::EvalError> {
    let ColumnDefault::Computed(computed) = default else {
        let ColumnDefault::Value(value) = default else {
            unreachable!("a default is settled or computed")
        };
        return materialize_stored_literal(
            value,
            field_type,
            column_info_version,
            flags,
            &ctx.time_zone(),
        )
        .map(|converted| converted.value)
        .map_err(|_| tidb_expr::EvalError::Unsupported("a stored DEFAULT the column cannot hold"));
    };
    let value = computed.expr.eval(ctx, row)?;
    if value.is_null() {
        return Ok(Datum::Null);
    }
    // Go `CastColumnValue`: the computed value is cast into the column's own
    // type before it is stored, which is what gives a `TIMESTAMP(0)` column a
    // second-resolution reading of a clock that has more.
    value
        .convert_to_in(field_type, flags, &ctx.time_zone())
        .map(|converted| converted.value)
        // A default whose computed value does not fit its own column is
        // refused at DDL time by Go's own argument checks, so this is
        // unreachable for a table this tier built.
        .map_err(|_| tidb_expr::EvalError::Unsupported("a computed DEFAULT the column cannot hold"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tidb_datatype::{CoreTime, Time, TimeType, STRICT_FLAGS};
    use tidb_model::column::COLUMN_INFO_VERSION0;

    fn fixed_zone(name: &str, offset_secs: i32) -> SessionTimeZone {
        SessionTimeZone::Fixed {
            name: name.to_owned(),
            offset_secs,
        }
    }

    fn materialized_time_text(converted: &Converted<Datum>) -> String {
        match &converted.value {
            Datum::Time(time) => time.to_string(),
            other => panic!("expected materialized time, got {other:?}"),
        }
    }

    #[test]
    fn version1_timestamp_uses_stored_utc_then_consumer_zone() {
        // Written as 08:00 in a +08 session, the DDL path persists this UTC
        // wall clock. Reading it must recover the same instant everywhere.
        let stored = Datum::new_string("2020-01-02 00:00:00");
        let field_type = FieldType::new(FieldTypeCode::Timestamp);
        let utc = SessionTimeZone::utc();
        let plus_eight = fixed_zone("+08:00", 8 * 60 * 60);

        for (consumer, expected) in [
            (&utc, "2020-01-02 00:00:00"),
            (&plus_eight, "2020-01-02 08:00:00"),
        ] {
            let converted = materialize_stored_literal(
                &stored,
                &field_type,
                COLUMN_INFO_VERSION1,
                STRICT_FLAGS,
                consumer,
            )
            .unwrap();
            assert_eq!(materialized_time_text(&converted), expected);
            assert_eq!(converted.event, None);
        }
    }

    #[test]
    fn version0_timestamp_uses_injected_system_zone() {
        let stored = Datum::new_string("2020-01-02 08:00:00");
        let field_type = FieldType::new(FieldTypeCode::Timestamp);
        let system_plus_eight = fixed_zone("System", 8 * 60 * 60);
        let converted = materialize_stored_literal_with_system_zone(
            &stored,
            &field_type,
            COLUMN_INFO_VERSION0,
            STRICT_FLAGS,
            &SessionTimeZone::utc(),
            || system_plus_eight.clone(),
        )
        .unwrap();

        assert_eq!(materialized_time_text(&converted), "2020-01-02 00:00:00");
    }

    #[test]
    fn zero_timestamp_is_not_projected() {
        let stored = Datum::new_string("0000-00-00 00:00:00");
        let field_type = FieldType::new(FieldTypeCode::Timestamp);
        let converted = materialize_stored_literal(
            &stored,
            &field_type,
            COLUMN_INFO_VERSION1,
            tidb_datatype::DEFAULT_STATEMENT_FLAGS,
            &fixed_zone("+08:00", 8 * 60 * 60),
        )
        .unwrap();

        assert_eq!(materialized_time_text(&converted), "0000-00-00 00:00:00");
    }

    #[test]
    fn non_timestamp_uses_consumer_conversion_and_preserves_event() {
        let field_type = FieldType::new(FieldTypeCode::LongLong);
        let temporal = Datum::new_time(
            Time::new(
                CoreTime::from_date(2011, 3, 13, 1, 59, 59, 999_999),
                TimeType::DateTime,
                6,
            )
            .unwrap(),
        );
        let utc = SessionTimeZone::utc();
        let los_angeles = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        for (consumer, expected) in [
            (&utc, Datum::Int(20_110_313_020_000)),
            (&los_angeles, Datum::Int(20_110_313_030_000)),
        ] {
            let converted = materialize_stored_literal(
                &temporal,
                &field_type,
                COLUMN_INFO_VERSION1,
                STRICT_FLAGS,
                consumer,
            )
            .unwrap();
            assert_eq!(converted.value, expected);
        }

        let stored = Datum::new_string("12x");
        let consumer = fixed_zone("+08:00", 8 * 60 * 60);
        let expected = stored
            .convert_to_in(&field_type, STRICT_FLAGS, &consumer)
            .unwrap();
        assert_eq!(
            expected.event,
            Some(tidb_datatype::ScalarConversionEvent::Truncated)
        );

        assert_eq!(
            materialize_stored_literal(
                &stored,
                &field_type,
                COLUMN_INFO_VERSION1,
                STRICT_FLAGS,
                &consumer,
            )
            .unwrap(),
            expected
        );
    }

    #[test]
    fn timestamp_projection_preserves_conversion_event() {
        let stored = Datum::new_time(
            Time::new(
                CoreTime::from_date(2011, 3, 13, 2, 30, 0, 0),
                TimeType::DateTime,
                0,
            )
            .unwrap(),
        );
        let field_type = FieldType::new(FieldTypeCode::Timestamp);
        let system_zone = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        let converted = materialize_stored_literal_with_system_zone(
            &stored,
            &field_type,
            COLUMN_INFO_VERSION0,
            STRICT_FLAGS,
            &SessionTimeZone::utc(),
            || system_zone.clone(),
        )
        .unwrap();

        assert_eq!(materialized_time_text(&converted), "2011-03-13 10:00:00");
        assert_eq!(
            converted.event,
            Some(tidb_datatype::ScalarConversionEvent::Truncated)
        );
    }

    #[test]
    fn timezone_projection_failure_is_a_datum_comparison_error() {
        // A typed timestamp can carry a wall clock in a spring-forward gap;
        // conversion into the type is exact, and source-zone projection is
        // the operation that must expose the timezone failure.
        let stored = Datum::new_time(
            Time::new(
                CoreTime::from_date(2011, 3, 13, 2, 30, 0, 0),
                TimeType::Timestamp,
                0,
            )
            .unwrap(),
        );
        let field_type = FieldType::new(FieldTypeCode::Timestamp);
        let system_zone = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        let error = materialize_stored_literal_with_system_zone(
            &stored,
            &field_type,
            COLUMN_INFO_VERSION0,
            STRICT_FLAGS,
            &SessionTimeZone::utc(),
            || system_zone.clone(),
        )
        .unwrap_err();

        assert!(matches!(error, DatumValueError::Comparison(_)));
    }
}
