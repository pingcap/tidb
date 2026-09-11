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

//! Go extractFunctionClass and its datetime/duration signatures.

use crate::{Columns, Datum, EvalError};
use tidb_datatype::{FieldType, FieldTypeCode};

fn number(result: Result<i64, tidb_datatype::TimeError>, unit: &str) -> Result<Datum, EvalError> {
    result.map(Datum::Int).map_err(|_| {
        EvalError::Conversion(tidb_error::terror::TerrorError::compatible(
            tidb_error::terror::TerrorCode::new(1105),
            format!("invalid unit {unit}"),
        ))
    })
}

pub(crate) fn extract(
    unit: &str,
    value: &Datum,
    source: Option<&FieldType>,
    ctx: &dyn Columns,
) -> Result<Datum, EvalError> {
    let clock = tidb_datatype::is_clock_unit(unit);
    let date = tidb_datatype::is_date_unit(unit);
    let datetime = source.is_some_and(|ty| {
        matches!(
            ty.code(),
            FieldTypeCode::Date | FieldTypeCode::Datetime | FieldTypeCode::Timestamp
        )
    }) || matches!(value, Datum::Time(_));
    if !clock || (date && datetime) {
        return match crate::cast::cast_arg_as_datetime(value, source, ctx)? {
            Datum::Null => Ok(Datum::Null),
            Datum::Time(time) => number(tidb_datatype::extract_datetime_num(time, unit), unit),
            _ => unreachable!("datetime argument cast returns a time or NULL"),
        };
    }
    // Go's mixed DAY_* string signature first parses duration, then prefers
    // datetime only when the clock fields agree and the year is positive.
    if date && !matches!(value, Datum::Duration(_)) {
        let text = crate::cast::cast_arg_as_string(value, source, ctx)?;
        let Some(text) = crate::coerce::coerce_str(&text)? else {
            return Ok(Datum::Null);
        };
        let invalid = || {
            EvalError::Conversion(
                tidb_datatype::ERR_TRUNCATED_WRONG_VALUE
                    .generate(format!("Truncated incorrect time value: '{text}'")),
            )
        };
        let parsed_duration = tidb_datatype::parse_mysql_duration(
            &text,
            i64::from(tidb_datatype::get_fsp(&text)),
            &chrono::Utc,
            true,
            ctx.date_modes().allow_invalid_dates,
        )
        .map_err(|_| invalid())?;
        if parsed_duration.overflow().is_some() || parsed_duration.truncated() {
            return Err(invalid());
        }
        let duration = tidb_datatype::MySqlDuration::from_nanoseconds(
            parsed_duration.nanoseconds(),
            parsed_duration.fsp(),
        )
        .map_err(|_| invalid())?;
        if let Ok(parsed) = tidb_datatype::parse_datetime(
            &text,
            &chrono::Utc,
            true,
            ctx.date_modes().allow_invalid_dates,
        ) {
            let core = parsed.time.core_time();
            if core.year() > 0
                && i64::from(core.hour()) == duration.hour()
                && i64::from(core.minute()) == duration.minute()
                && i64::from(core.second()) == duration.second()
            {
                return number(tidb_datatype::extract_datetime_num(parsed.time, unit), unit);
            }
        }
        return number(tidb_datatype::extract_duration_num(duration, unit), unit);
    }
    match crate::cast::cast_arg_as_duration(value, source, ctx)? {
        Datum::Null => Ok(Datum::Null),
        Datum::Duration(duration) => {
            number(tidb_datatype::extract_duration_num(duration, unit), unit)
        }
        _ => unreachable!("duration argument cast returns a duration or NULL"),
    }
}
