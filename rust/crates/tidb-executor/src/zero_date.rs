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

//! What a zero, zero-in, or invalid DATE/DATETIME/TIMESTAMP written into a
//! column DOES, and the value that stands in for it when it is not an error.
//!
//! Mirrors Go `pkg/table/column.go`'s `handleZeroDatetime`, and the
//! conversion flags `pkg/util/misc.go`'s `GetTypeFlagsForInsert` and
//! `pkg/executor/select.go`'s `ResetUpdateStmtCtx` derive from the SQL mode.
//!
//! # Four inputs, and none of them is "strict mode" alone
//!
//! `NO_ZERO_DATE`, `NO_ZERO_IN_DATE` and `ALLOW_INVALID_DATES` each decide a
//! DIFFERENT question, and strict mode only decides the LEVEL of whatever
//! they turn up. The two zero flags are exact mirrors of each other, which is
//! the fastest way to see they are not one bit:
//!
//! | `INSERT INTO t(v DATE)` | `NO_ZERO_DATE` alone | `NO_ZERO_IN_DATE` alone |
//! | --- | --- | --- |
//! | `'0000-00-00'` | warning 1292, stores `0000-00-00` | accepted, stores `0000-00-00` |
//! | `'2024-00-01'` | accepted, stores `2024-00-01` | warning 1292, stores `0000-00-00` |
//!
//! Add strict mode to either and the warning becomes an error. Take both
//! flags away and even strict mode stores both values silently -- captured,
//! `sql_mode = 'STRICT_TRANS_TABLES'` accepts `'0000-00-00'`.
//!
//! `ALLOW_INVALID_DATES` is about which dates EXIST, not about the level:
//! it makes `'2024-02-31'` a real value that stores as written, in strict
//! mode too, while `'2024-13-01'` stays wrong because a 13th month is not a
//! date at all. It reaches this seam through the conversion flags rather
//! than through the table below, because Go asks it inside `Time.Check`.
//!
//! # TIMESTAMP is stricter, and Go says so in a comment
//!
//! `handleZeroDatetime`'s timestamp arms come first and swallow every case
//! the DATE arms would tolerate. A TIMESTAMP that ends up zero because its
//! input was bad is a warning even when the input was merely a zero-in-date,
//! and `ALLOW_INVALID_DATES` does not rescue `'2024-02-31'` there. Only the
//! genuinely all-zero input gets the `NO_ZERO_DATE` treatment. Captured:
//!
//! | `INSERT INTO t(v TIMESTAMP)`, `sql_mode = ''` | |
//! | --- | --- |
//! | `'0000-00-00'` | accepted, stores the zero timestamp, no warning |
//! | `'2024-00-01'` | warning 1292, stores the zero timestamp |
//!
//! # The stored value is the zero of the column's own type
//!
//! Never NULL, and never the input: a warned DATE stores `0000-00-00` and a
//! warned DATETIME/TIMESTAMP stores `0000-00-00 00:00:00`. That is Go
//! returning `types.NewDatum(zeroV)` on both the warning and the error path,
//! and it is what makes the non-strict path a silent data change rather than
//! a rejection.

use tidb_datatype::{Datum, FieldTypeCode, Time};

/// The three SQL-mode bits that change what a temporal write means, carried
/// together because no path needs one without the others.
///
/// Go reads them off `SessionVars.SQLMode` as `HasNoZeroDateMode`,
/// `HasNoZeroInDateMode` and `HasAllowInvalidDatesMode`. All three are false
/// in `mysql.ModeNone`; TiDB's shipped `DefaultSQLMode` sets the first two.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DateModes {
    /// `NO_ZERO_DATE`.
    pub no_zero_date: bool,
    /// `NO_ZERO_IN_DATE`.
    pub no_zero_in_date: bool,
    /// `ALLOW_INVALID_DATES`.
    pub allow_invalid_dates: bool,
}

/// What the write path should do with one converted temporal value.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ZeroDateAction {
    /// Store this value with no diagnostic.
    Store(Datum),
    /// Store this value and leave warning 1292 behind.
    WarnAndStore(Datum),
    /// Fail the statement with 1292.
    Refuse,
}

/// Go `handleZeroDatetime`.
///
/// `converted` is the value `ConvertTo` produced -- which Go returns even
/// when it also returns an error, and which is the zero of the target type in
/// exactly that case. `was_invalid` is Go's `tmIsInvalid`, i.e. whether that
/// conversion reported `ErrWrongValue`.
pub(crate) fn handle_zero_datetime(
    code: FieldTypeCode,
    converted: Time,
    was_invalid: bool,
    modes: DateModes,
    strict: bool,
) -> ZeroDateAction {
    let zero = Datum::new_time(zero_of(converted));
    let store_zero = |warn: bool| {
        if warn && strict {
            ZeroDateAction::Refuse
        } else if warn {
            ZeroDateAction::WarnAndStore(zero.clone())
        } else {
            ZeroDateAction::Store(zero.clone())
        }
    };

    // Timestamp first, as Go does: its two arms shadow every case below.
    if code == FieldTypeCode::Timestamp {
        if converted.is_zero() {
            return store_zero(was_invalid || modes.no_zero_date);
        }
        if was_invalid {
            // An invalid timestamp is never stored as itself.
            return store_zero(true);
        }
    } else if converted.is_zero() {
        // Go: "Don't care NoZeroDate mode if time val is invalid."
        if !was_invalid && !modes.no_zero_date {
            return ZeroDateAction::Store(zero);
        }
        return store_zero(true);
    } else if converted.invalid_zero() {
        if !modes.no_zero_in_date {
            return ZeroDateAction::Store(Datum::new_time(converted));
        }
        return store_zero(true);
    }

    ZeroDateAction::Store(Datum::new_time(converted))
}

/// Go's `zeroV`: `ZeroDate`, `ZeroDatetime` or `ZeroTimestamp`, picked by the
/// column's type. Derived from the converted value so the fsp and kind that
/// the column asked for survive.
fn zero_of(converted: Time) -> Time {
    Time::new(
        tidb_datatype::CoreTime::default(),
        converted.kind(),
        i64::from(converted.fsp()),
    )
    .unwrap_or(converted)
}

/// Go `GetTypeFlagsForInsert` / `ResetUpdateStmtCtx`, in the two bits that
/// decide whether a zero-in or invalid date PARSES at all.
///
/// The `IgnoreZeroInDate` expression is Go's verbatim, and its shape is the
/// point: the zero-in-date is let through unless `NO_ZERO_IN_DATE` **and**
/// `NO_ZERO_DATE` **and** strict mode are all present and
/// `ALLOW_INVALID_DATES` is absent. Any one of those missing and
/// `'2024-00-01'` reaches [`handle_zero_datetime`] as a real value instead of
/// a conversion failure -- which is what makes `NO_ZERO_IN_DATE` alone a
/// warning rather than an error.
#[must_use]
pub(crate) fn write_date_flags(
    base: tidb_datatype::ConversionFlags,
    modes: DateModes,
    strict: bool,
) -> tidb_datatype::ConversionFlags {
    base.with_ignore_invalid_date_err(modes.allow_invalid_dates)
        .with_ignore_zero_in_date_err(
            !modes.no_zero_in_date || !modes.no_zero_date || !strict || modes.allow_invalid_dates,
        )
}
