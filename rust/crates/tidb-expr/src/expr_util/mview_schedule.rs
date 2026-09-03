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

//! `pkg/expression/helper.go`'s materialized-view schedule helpers (Go master
//! `94a9cbedab`): the shared conversion flags, error levels, and Unix-second
//! conversion used to build and evaluate materialized view schedule
//! expressions.

use tidb_datatype::{ConversionFlags, CoreTime, TimeConversionError};
use tidb_error::errctx::{resolve_err_level, LevelMap};
use tidb_model::job::ResolvedTimeZone;
use tidb_mysql::SqlMode;

/// Go `MaterializedScheduleTimeToUnixSeconds`: converts a materialized
/// schedule time interpreted in `scheduleTimeZone` to Unix seconds for
/// persisting in internal MV system tables. A `None` time persists `None`;
/// Go's `*time.Location` nil check maps to `None` for the resolved zone.
pub fn materialized_schedule_time_to_unix_seconds(
    time: Option<CoreTime>,
    schedule_time_zone: Option<&ResolvedTimeZone>,
) -> Result<Option<i64>, String> {
    let Some(time) = time else {
        return Ok(None);
    };
    let Some(zone) = schedule_time_zone else {
        return Err("materialized schedule timezone is unavailable".to_owned());
    };
    let unix_seconds = go_time_unix_seconds(time, zone).map_err(|error| format!("{error}"))?;
    Ok(Some(unix_seconds))
}

/// Go `types.Time.GoTime` under the resolved location, reduced to Unix
/// seconds.
fn go_time_unix_seconds(
    time: CoreTime,
    zone: &ResolvedTimeZone,
) -> Result<i64, TimeConversionError> {
    match zone {
        ResolvedTimeZone::Local => time
            .to_datetime(&chrono::Local)
            .map(|datetime| datetime.timestamp()),
        ResolvedTimeZone::Named(zone) => {
            time.to_datetime(zone).map(|datetime| datetime.timestamp())
        }
        ResolvedTimeZone::Fixed { offset_seconds, .. } => {
            let offset = chrono::FixedOffset::east_opt(i32::try_from(*offset_seconds).unwrap_or(0))
                .ok_or(TimeConversionError::InvalidCalendar)?;
            time.to_datetime(&offset)
                .map(|datetime| datetime.timestamp())
        }
    }
}

/// Go `MaterializedScheduleTypeFlagsWithSQLMode`: derives the type conversion
/// flags used to build and evaluate materialized view schedule expressions.
#[must_use]
pub fn materialized_schedule_type_flags_with_sql_mode(mode: SqlMode) -> ConversionFlags {
    tidb_datatype::STRICT_FLAGS
        .with_truncate_as_warning(!mode.has_strict_mode())
        .with_ignore_invalid_date_err(mode.has_allow_invalid_dates_mode())
        .with_ignore_zero_in_date_err(
            !mode.has_strict_mode() || mode.has_allow_invalid_dates_mode(),
        )
        .with_cast_time_to_year_through_concat(true)
}

/// Go `MaterializedScheduleErrLevelsWithSQLMode`: derives the error levels
/// used to build and evaluate materialized view schedule expressions.
#[must_use]
pub fn materialized_schedule_err_levels_with_sql_mode(mode: SqlMode) -> LevelMap {
    LevelMap::strict()
        .with_level(
            tidb_error::errctx::ErrGroup::Truncate,
            resolve_err_level(false, !mode.has_strict_mode()),
        )
        .with_level(
            tidb_error::errctx::ErrGroup::BadNull,
            resolve_err_level(false, !mode.has_strict_mode()),
        )
        .with_level(
            tidb_error::errctx::ErrGroup::NoDefault,
            resolve_err_level(false, !mode.has_strict_mode()),
        )
        .with_level(
            tidb_error::errctx::ErrGroup::DividedByZero,
            resolve_err_level(
                !mode.has_error_for_division_by_zero_mode(),
                !mode.has_strict_mode(),
            ),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mode_bits(flags: &[SqlMode]) -> SqlMode {
        flags
            .iter()
            .copied()
            .fold(SqlMode::default(), std::ops::BitOr::bitor)
    }

    fn time_at(year: u16, month: u8, day: u8, hour: u8, minute: u8, second: u8) -> CoreTime {
        CoreTime::from_date(year, month, day, hour, minute, second, 0)
    }

    #[test]
    fn nil_time_and_missing_zone_boundaries() {
        assert_eq!(
            materialized_schedule_time_to_unix_seconds(None, None).expect("nil time persists nil"),
            None
        );
        let error =
            materialized_schedule_time_to_unix_seconds(Some(time_at(2026, 1, 2, 3, 4, 5)), None)
                .expect_err("a missing zone refuses the conversion");
        assert_eq!(error, "materialized schedule timezone is unavailable");
    }

    #[test]
    fn schedule_time_converts_under_the_given_zone() {
        let zone = ResolvedTimeZone::Fixed {
            name: "Manual".into(),
            offset_seconds: 8 * 3600,
        };
        // Go `t.GoTime(zone).Unix()`: the wall clock 08:00 in +08:00 is the
        // same instant as 00:00 UTC of the same date.
        use chrono::TimeZone as _;
        let expected = chrono::Utc
            .with_ymd_and_hms(2026, 1, 2, 0, 0, 0)
            .unwrap()
            .timestamp();
        let unix = materialized_schedule_time_to_unix_seconds(
            Some(time_at(2026, 1, 2, 8, 0, 0)),
            Some(&zone),
        )
        .expect("valid conversion")
        .expect("some seconds");
        assert_eq!(unix, expected);

        let utc = ResolvedTimeZone::Named(chrono_tz::Tz::UTC);
        let unix_utc = materialized_schedule_time_to_unix_seconds(
            Some(time_at(2026, 1, 2, 0, 0, 0)),
            Some(&utc),
        )
        .expect("valid conversion")
        .expect("some seconds");
        assert_eq!(unix_utc, expected, "same instant under the matching zone");
    }

    #[test]
    fn type_flags_follow_the_sql_mode() {
        // Strict mode (default SQL mode bundle): truncation kept as errors,
        // zero-in-date rejected.
        let strict = materialized_schedule_type_flags_with_sql_mode(mode_bits(&[
            tidb_mysql::consts::ModeStrictAllTables,
        ]));
        assert!(!strict.truncate_as_warning());
        assert!(!strict.ignore_invalid_date_err());
        assert!(!strict.ignore_zero_in_date_err());
        assert!(strict.cast_time_to_year_through_concat());

        // Non-strict with invalid dates allowed: both relaxations on.
        let relaxed = materialized_schedule_type_flags_with_sql_mode(mode_bits(&[
            tidb_mysql::consts::ModeAllowInvalidDates,
        ]));
        assert!(relaxed.truncate_as_warning());
        assert!(relaxed.ignore_invalid_date_err());
        assert!(relaxed.ignore_zero_in_date_err());
        assert!(relaxed.cast_time_to_year_through_concat());
    }

    #[test]
    fn err_levels_follow_the_sql_mode() {
        let strict = materialized_schedule_err_levels_with_sql_mode(mode_bits(&[
            tidb_mysql::consts::ModeStrictAllTables,
        ]));
        assert_eq!(
            strict.get(tidb_error::errctx::ErrGroup::Truncate),
            tidb_error::errctx::Level::Error
        );
        assert_eq!(
            strict.get(tidb_error::errctx::ErrGroup::BadNull),
            tidb_error::errctx::Level::Error
        );
        assert_eq!(
            strict.get(tidb_error::errctx::ErrGroup::NoDefault),
            tidb_error::errctx::Level::Error
        );
        // Go: `resolve_err_level(!HasErrorForDivisionByZeroMode(), false)`:
        // STRICT_ALL_TABLES alone lacks the division flag, so `ignore=true`
        // wins and divided-by-zero resolves to Ignore.
        assert_eq!(
            strict.get(tidb_error::errctx::ErrGroup::DividedByZero),
            tidb_error::errctx::Level::Ignore
        );

        let relaxed = materialized_schedule_err_levels_with_sql_mode(mode_bits(&[
            tidb_mysql::consts::ModeAllowInvalidDates,
        ]));
        assert_eq!(
            relaxed.get(tidb_error::errctx::ErrGroup::Truncate),
            tidb_error::errctx::Level::Warn
        );
        assert_eq!(
            relaxed.get(tidb_error::errctx::ErrGroup::BadNull),
            tidb_error::errctx::Level::Warn
        );
        assert_eq!(
            relaxed.get(tidb_error::errctx::ErrGroup::NoDefault),
            tidb_error::errctx::Level::Warn
        );
        // Go's ResolveErrLevel lets `ignore` win over `warn`: without the
        // division flag the level is Ignore even non-strict.
        assert_eq!(
            relaxed.get(tidb_error::errctx::ErrGroup::DividedByZero),
            tidb_error::errctx::Level::Ignore
        );
    }
}
