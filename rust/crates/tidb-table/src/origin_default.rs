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

//! Go `table.GetColOriginDefaultValue` and the literal DEFAULT of a stored
//! column: the value a stored `ColumnInfo`'s default materialises to.

#[allow(unused_imports)]
use tidb_datatype::{
    new_collation_enabled, parse_enum, parse_set_name, Datum, FieldType, FieldTypeCode, GoString,
    SessionTimeZone, Time,
};
use tidb_datatype::RuntimeCollator;
use tidb_model::{GoAny, GoAnyView};
use tidb_model::column::ColumnInfo;

/// Go `ast.CurrentTimestamp`, as a column default stores it.
pub const CURRENT_TIMESTAMP: &str = "CURRENT_TIMESTAMP";

pub fn encode_error(detail: impl Into<String>) -> RowEncodeError {
    RowEncodeError(detail.into())
}

/// The datum one column's declared `DEFAULT` materialises to when the caller
/// has no statement clock to evaluate a functional default against.
///
/// [`declared_default`] answers `CURRENT_TIMESTAMP` with the caller's `now`,
/// which is right for a writer materialising one row at one instant and wrong
/// for a *catalog loader*, which would freeze that instant into every future
/// row. This one therefore refuses every non-literal default -- an expression
/// default, and `CURRENT_TIMESTAMP` -- so a loader can admit exactly the
/// columns whose DEFAULT it can carry across verbatim and refuse the rest by
/// name instead of silently dropping it.
pub fn literal_default(column: &ColumnInfo, table: &str) -> Result<Datum, RowEncodeError> {
    if column.default_is_expr {
        return Err(unmaterialisable(column, table));
    }
    let declared = column.get_default_value();
    if let Some(GoAnyView::String(bytes)) = declared.view() {
        if is_time_column(column)
            && bytes
                .as_bytes()
                .eq_ignore_ascii_case(CURRENT_TIMESTAMP.as_bytes())
        {
            return Err(unmaterialisable(column, table));
        }
    }
    materialise(column, &declared, table, None)
}

/// The datum one column's `OriginDefaultValue` materialises to.
///
/// This is what a *read* substitutes for a column the stored row has no entry
/// for at all, which is every row written before an `ALTER TABLE ... ADD
/// COLUMN`: Go encodes it into the coprocessor request
/// (`tables.SetPBColumnsDefaultValue` ->
/// `GetColOriginDefaultValueWithoutStrictSQLMode`), so the scan -- and the
/// `ANALYZE` reading through it -- sees the default rather than NULL.
///
/// A nil interface is Go's nil `OriginDefaultValue`, whose read *is* NULL
/// (`pkg/table/column.go`'s `getColDefaultValueFromNil`). Unlike a declared
/// default this one is always a literal: the DDL that added the column
/// evaluated any expression once, at that moment, and stored the result.
pub fn origin_default(column: &ColumnInfo, table: &str) -> Result<Datum, RowEncodeError> {
    let default = column.get_origin_default_value();
    // Go `getColDefaultValue` reads a time column's origin through
    // `GetTimeValue`, which evaluates the words `CURRENT_TIMESTAMP` and
    // `CURRENT_DATE` (the date part only) rather than parsing them.
    if let (Some(time_type), Some(tidb_model::GoAnyView::String(bytes))) = (
        match column.get_type() {
            FieldTypeCode::Date | FieldTypeCode::NewDate => Some(tidb_datatype::TimeType::Date),
            FieldTypeCode::Datetime => Some(tidb_datatype::TimeType::DateTime),
            FieldTypeCode::Timestamp => Some(tidb_datatype::TimeType::Timestamp),
            _ => None,
        },
        default.view(),
    ) {
        let word = bytes.as_bytes();
        let current_date = word.eq_ignore_ascii_case(b"CURRENT_DATE");
        if current_date || word.eq_ignore_ascii_case(CURRENT_TIMESTAMP.as_bytes()) {
            use chrono::{Datelike, Timelike};
            let now = chrono::Local::now();
            let fsp = if time_type == tidb_datatype::TimeType::Date {
                0
            } else {
                column.field_type.decimal().max(0)
            };
            let (hour, minute, second, micro) = if current_date {
                (0, 0, 0, 0)
            } else {
                (
                    now.hour(),
                    now.minute(),
                    now.second(),
                    now.nanosecond() / 1000,
                )
            };
            let micro = if fsp == 0 {
                0
            } else {
                micro / 10u32.pow(6 - fsp as u32) * 10u32.pow(6 - fsp as u32)
            };
            let time = Time::from_date_checked(
                now.year(),
                now.month() as i32,
                now.day() as i32,
                hour as i32,
                minute as i32,
                second as i32,
                micro as i32,
                time_type,
                i64::from(fsp),
            )
            .map_err(|_| unmaterialisable(column, table))?;
            return Ok(Datum::new_time(time));
        }
    }
    let datum = materialise(column, &default, table, None)?;
    // Go `GetColOriginDefaultValueWithoutStrictSQLMode` -> `getColDefaultValue`
    // casts the stored text to the column's type (`CastColumnValue`,
    // non-strict; a time type through `GetTimeValue`), for EVERY type: a
    // region fills a DATE column with a date, a JSON column with a binary
    // document and an ENUM column with a member -- never with the bytes of
    // their printed form, which no reader of that column can hold.
    let (Datum::Bytes(_) | Datum::String(_)) = &datum else {
        return Ok(datum);
    };
    // Without strict SQL mode a zero-in or invalid date is kept, not refused:
    // `ADD COLUMN v DATE DEFAULT '2024-00-01'` under sql_mode '' reads back as
    // written even from a strict session.
    let flags = tidb_datatype::ConversionFlags::from_bits(
        tidb_datatype::ConversionFlags::IGNORE_TRUNCATE_ERR
            | tidb_datatype::ConversionFlags::ALLOW_NEGATIVE_TO_UNSIGNED
            | tidb_datatype::ConversionFlags::IGNORE_ZERO_DATE_ERR
            | tidb_datatype::ConversionFlags::IGNORE_ZERO_IN_DATE_ERR
            | tidb_datatype::ConversionFlags::IGNORE_INVALID_DATE_ERR,
    );
    datum
        .convert_to(&column.field_type, flags)
        .map(|converted| converted.value)
        .map_err(|_| unmaterialisable(column, table))
}

/// Go `getColDefaultValue` evaluates the clock words through `GetTimeValue`
/// for these types alone; any other column's `CURRENT_TIMESTAMP` default is
/// that text, cast like every other literal.
pub fn is_time_column(column: &ColumnInfo) -> bool {
    matches!(
        column.get_type(),
        FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
    )
}

pub fn unmaterialisable(column: &ColumnInfo, table: &str) -> RowEncodeError {
    encode_error(format!(
        "{table}.{} declares a default this path cannot materialise",
        column.name.original()
    ))
}

/// The shared literal-to-datum rule. `current_timestamp` is `Some` only for a
/// declared default, the one kind that may still spell `CURRENT_TIMESTAMP`.
pub fn materialise(
    column: &ColumnInfo,
    declared: &GoAny,
    table: &str,
    current_timestamp: Option<Time>,
) -> Result<Datum, RowEncodeError> {
    let refuse = || unmaterialisable(column, table);
    // A `TIMESTAMP DEFAULT CURRENT_TIMESTAMP` column stores that very word as
    // its default: an `INSERT` evaluates it, so a writer that stores the word
    // instead writes a row TiDB rejects as an `Incorrect time value`.
    if let (Some(GoAnyView::String(bytes)), Some(now)) = (declared.view(), current_timestamp) {
        if is_time_column(column)
            && bytes
                .as_bytes()
                .eq_ignore_ascii_case(CURRENT_TIMESTAMP.as_bytes())
        {
            return Ok(Datum::new_time(now));
        }
    }
    let Some(default) = declared.view() else {
        // No declared default: an `INSERT` stores NULL, and the column is
        // nullable or the schema would not have parsed.
        return Ok(Datum::Null);
    };
    let datum = match default {
        GoAnyView::Int(value) => Datum::Int(value),
        GoAnyView::Uint(value) => Datum::UInt(value),
        GoAnyView::Bool(value) => Datum::Int(i64::from(value)),
        GoAnyView::Float(value) => Datum::Real(value),
        GoAnyView::String(bytes) => {
            // Go `types.NewDatum` sets a dynamic string with
            // `mysql.DefaultCollationName`: it is UTF-8 text, so a later cast
            // into a GBK column validates it rather than decoding it as GBK.
            let text = Datum::new_collation_string(
                bytes.as_bytes().to_vec(),
                tidb_datatype::Collation::Utf8Mb4Bin,
            );
            // A numeric column's default is stored as its printed form, so it
            // has to be read back as a number before it is encoded as one.
            match column.get_type() {
                FieldTypeCode::Tiny
                | FieldTypeCode::Short
                | FieldTypeCode::Int24
                | FieldTypeCode::Long
                | FieldTypeCode::LongLong
                | FieldTypeCode::Year => {
                    let printed = std::str::from_utf8(bytes.as_bytes()).map_err(|_| refuse())?;
                    // An unsigned default reads in the unsigned domain: a
                    // `BIGINT UNSIGNED DEFAULT '12818995484625953384'` is past
                    // `i64::MAX`, and refusing it made the session skip the
                    // whole table, which then did not exist for it.
                    if column
                        .field_type
                        .has_flag(tidb_datatype::FieldTypeFlags::UNSIGNED)
                    {
                        Datum::UInt(printed.trim().parse::<u64>().map_err(|_| refuse())?)
                    } else {
                        Datum::Int(printed.trim().parse::<i64>().map_err(|_| refuse())?)
                    }
                }
                _ => typed_value(&text, &column.field_type)?,
            }
        }
        // `types.NewDatum` accepts a dynamic `[]byte` without converting it
        // through text. Copy the visible Go slice bytes while retaining the
        // nil-interface/typed-nil distinction: a typed nil `[]byte` is an
        // empty byte datum, not SQL NULL.
        GoAnyView::Bytes(bytes) => Datum::Bytes(bytes.header().snapshot()),
        GoAnyView::Byte(_)
        | GoAnyView::DefinedString(_, _)
        | GoAnyView::Slice(_)
        | GoAnyView::Map(_)
        | GoAnyView::Pointer(_)
        | GoAnyView::Array(_)
        | GoAnyView::Struct(_)
        | GoAnyView::Custom => return Err(refuse()),
    };
    Ok(datum)
}

/// Why one row could not be encoded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RowEncodeError(pub String);

impl std::fmt::Display for RowEncodeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RowEncodeError {}

/// Re-types a value for the column it lands in.
///
/// Every value a caller states is spelled as text, because that is how Go's
/// own `INSERT` spells it; a column whose declared type is `ENUM` or `SET`
/// needs it as the member(s) it names, not as a string, or the row decodes as
/// the wrong type. Both carry the element's numeric position as well as its
/// name, and that number comes from the column's own declaration -- so a value
/// naming an element the column does not declare is refused rather than
/// written with an invented position.
pub fn typed_value(value: &Datum, field_type: &FieldType) -> Result<Datum, RowEncodeError> {
    let bytes = match value {
        Datum::Bytes(bytes) => bytes.as_slice(),
        Datum::String(_) => value.as_raw_bytes().expect("a string datum carries bytes"),
        _ => return Ok(value.clone()),
    };
    let code = field_type.code();
    if !matches!(code, FieldTypeCode::Enum | FieldTypeCode::Set) {
        return Ok(value.clone());
    }
    let diagnostic_name = GoString::from(bytes).to_utf8_lossy_go();
    if field_type.elems().is_empty() {
        return Err(RowEncodeError(format!(
            "a {code:?} column that declares no elements cannot store `{diagnostic_name}`"
        )));
    }
    // The stored spelling is the declared one, so name matching runs under the
    // column's own collation -- which is how Go's `ParseEnum`/`ParseSet` do it.
    let collator = field_type.runtime_collator();
    let datum_collation = field_type.collation();
    if code == FieldTypeCode::Enum {
        let member = field_type
            .with_elems_visible(|elements| parse_enum(elements, bytes, collator))
            .map_err(|error| RowEncodeError(error.to_string()))?;
        Ok(Datum::new_enum(member, datum_collation))
    } else {
        // Go stores a SET as the declaration-ordered bit mask plus the joined
        // names; `parse_set_name` computes both, and answers the empty set for
        // the empty string rather than treating it as an unknown element.
        let members = field_type
            .with_elems_visible(|elements| parse_set_name(elements, bytes, collator))
            .map_err(|error| RowEncodeError(error.to_string()))?;
        Ok(Datum::new_set(members, datum_collation))
    }
}
