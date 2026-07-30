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

//! What a NULL written into a `NOT NULL` column DOES, and the value that
//! stands in for it when it is not an error.
//!
//! Mirrors Go `pkg/table/column.go`'s `CheckNotNull`, `HandleBadNull` and
//! `GetZeroValue`, and the LEVEL those run at, which
//! `pkg/executor/select.go` derives per statement kind
//! (`ResetContextOfStmt`'s `ast.InsertStmt` arm, `ResetUpdateStmtCtx`,
//! `ResetDeleteStmtCtx`).
//!
//! # The level is not "strict mode", and that is the whole subtlety
//!
//! A NOT NULL violation is an ERROR or a WARNING depending on the statement,
//! and the SQL mode is only one of the two inputs. Go:
//!
//! ```text
//! INSERT:  error  <=  (strict || the VALUES list has exactly one row) && !IGNORE
//! UPDATE:  error  <=  strict && !IGNORE
//! DELETE:  error  <=  strict && !IGNORE
//! ```
//!
//! The single-row INSERT clause is MySQL's own rule -- "for single-row
//! inserts, ignore non-strict mode", the reference manual's
//! `constraint-invalid-data` page -- and it is why the two statements below
//! disagree under `sql_mode = ''`. Captured from TiDB:
//!
//! | `sql_mode = ''` | TiDB |
//! | --- | --- |
//! | `INSERT INTO t VALUES (NULL)`, `t(a INT NOT NULL)` | 1048, nothing stored |
//! | `INSERT INTO t VALUES (NULL),(2)` | accepted, warning 1048, stores `0` then `2` |
//! | `UPDATE t SET a = NULL` | accepted, warning 1048 per row, stores `0` |
//! | `INSERT INTO t (b) VALUES (9)`, `a` NOT NULL with no default | accepted, warning 1364, stores `0` |
//!
//! Under the default strict mode all four are errors (1048, 1048, 1048,
//! 1364). Modelling this as "strict" alone would get the second and third
//! rows wrong in OPPOSITE directions, which is why the level is a parameter
//! of [`handle_bad_null`] rather than a read of the statement context.
//!
//! # The stand-in value is per TYPE, not `0`
//!
//! [`zero_value`] is Go `GetZeroValue`: the type's own zero, not an integer
//! cast into the column. Captured from TiDB with
//! `a VARCHAR(5), b DATE, c DECIMAL(6,2), d DOUBLE` all NOT NULL, updated to
//! NULL under `sql_mode = ''`: `''`, `0000-00-00`, `0.00`, `0`. The decimal
//! keeps the column's SCALE, so the zero is rounded to it rather than being
//! the bare integer zero.

use tidb_datatype::{
    BinaryJSON, BinaryJSONValue, BinaryLiteral, CoreTime, Datum, Decimal, FieldType, FieldTypeCode,
    MySqlDuration, MysqlEnum, MysqlSet, StringDatum, Time, TimeType,
};

use crate::driver::DriverError;

/// Go `mysql.NotNullFlag`.
const NOT_NULL_FLAG: u32 = 1;
/// Go `mysql.UnsignedFlag`.
const UNSIGNED_FLAG: u32 = 32;

/// Whether a NOT NULL violation fails the statement or is downgraded to a
/// warning plus the type's zero value -- Go's `errctx.Level` for
/// `ErrGroupBadNull` / `ErrGroupNoDefault`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NullLevel {
    /// Go `errctx.LevelError`.
    Error,
    /// Go `errctx.LevelWarn`.
    Warn,
}

impl NullLevel {
    /// Go `errctx.ResolveErrLevel(false, warn)`.
    pub(crate) fn from_is_error(is_error: bool) -> Self {
        if is_error {
            Self::Error
        } else {
            Self::Warn
        }
    }
}

/// Go `Column.HandleBadNull`: a NULL in a NOT NULL column either fails the
/// statement or becomes that column's zero value plus a warning.
///
/// Returns whether the value was replaced, so a caller that tracks "did this
/// row change" sees the substitution.
pub(crate) fn handle_bad_null(
    value: &mut Datum,
    field_type: &FieldType,
    column: &str,
    level: NullLevel,
    ctx: &crate::StmtContext,
) -> Result<bool, DriverError> {
    if !value.is_null() || field_type.flags() & NOT_NULL_FLAG == 0 {
        return Ok(false);
    }
    if level == NullLevel::Error {
        return Err(DriverError::ColumnCannotBeNull(column.to_owned()));
    }
    ctx.append_warning_parts(1048, &format!("Column '{column}' cannot be null"));
    *value = zero_value(field_type);
    Ok(true)
}

/// Go `table.GetZeroValue`: the value a column takes when its NULL is
/// tolerated rather than refused.
///
/// A type this tier does not otherwise model reaches the `Null` arm exactly
/// as Go's `switch` falls through to its zero `types.Datum`, which is also
/// NULL -- so the fallback is Go's own, not a gap papered over.
pub(crate) fn zero_value(field_type: &FieldType) -> Datum {
    let unsigned = field_type.flags() & UNSIGNED_FLAG != 0;
    let collation = field_type.collation();
    match field_type.code() {
        FieldTypeCode::Tiny
        | FieldTypeCode::Short
        | FieldTypeCode::Int24
        | FieldTypeCode::Long
        | FieldTypeCode::LongLong => {
            if unsigned {
                Datum::UInt(0)
            } else {
                Datum::Int(0)
            }
        }
        FieldTypeCode::Year => Datum::Int(0),
        FieldTypeCode::Float => Datum::Float32(0.0),
        FieldTypeCode::Double => Datum::Real(0.0),
        // Go sets the datum's length and frac from the column, which is what
        // makes a DECIMAL(6,2) read back as `0.00` and not `0`.
        FieldTypeCode::NewDecimal => {
            Datum::new_decimal(Decimal::from_int(0).round_to_scale(field_type.decimal() as i32))
        }
        FieldTypeCode::String
        | FieldTypeCode::VarString
        | FieldTypeCode::Varchar
        | FieldTypeCode::Blob
        | FieldTypeCode::TinyBlob
        | FieldTypeCode::MediumBlob
        | FieldTypeCode::LongBlob => Datum::String(StringDatum::new(Vec::new(), collation)),
        FieldTypeCode::Duration => Datum::new_duration(
            MySqlDuration::from_nanoseconds(0, field_type.decimal())
                .unwrap_or_else(|_| MySqlDuration::from_nanoseconds(0, 0).expect("fsp 0 is valid")),
        ),
        FieldTypeCode::Date | FieldTypeCode::NewDate => zero_time(TimeType::Date, 0),
        FieldTypeCode::Datetime => zero_time(TimeType::DateTime, field_type.decimal()),
        FieldTypeCode::Timestamp => zero_time(TimeType::Timestamp, field_type.decimal()),
        FieldTypeCode::Bit => Datum::Bit(BinaryLiteral::from_uint(0, None)),
        FieldTypeCode::Set => Datum::new_set(MysqlSet::new(String::new(), 0), collation),
        FieldTypeCode::Enum => Datum::new_enum(MysqlEnum::new(String::new(), 0), collation),
        FieldTypeCode::Json => BinaryJSON::from_typed_value(&BinaryJSONValue::Null)
            .map_or(Datum::Null, Datum::new_json),
        _ => Datum::Null,
    }
}

/// The all-zero calendar value of one temporal kind: Go's `types.ZeroDate`,
/// `ZeroDatetime` and `ZeroTimestamp`, which share the raw core `0`.
fn zero_time(kind: TimeType, fsp: i64) -> Datum {
    Time::new(CoreTime::from_raw(0), kind, fsp)
        .or_else(|_| Time::new(CoreTime::from_raw(0), kind, 0))
        .map_or(Datum::Null, Datum::new_time)
}
