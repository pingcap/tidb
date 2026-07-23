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

use std::cmp::Ordering;
use std::fmt;

use chrono::{DateTime, Datelike, Duration as ChronoDuration, Local, TimeZone, Timelike, Utc};

use crate::{
    check_fsp, get_last_day, CoreTime, Decimal, FspError, MySqlDuration, PackedTime,
    TimeConversionError,
};

const MONTH_NAMES: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];

/// MySQL temporal type carried by [`Time`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TimeType {
    /// `DATE`.
    Date,
    /// `DATETIME`.
    DateTime,
    /// `TIMESTAMP`.
    Timestamp,
}

/// TiDB date/datetime/timestamp value.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Time {
    core: CoreTime,
    kind: TimeType,
    fsp: u8,
}

/// Parsed trailing timezone fields from a temporal literal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TimezoneSuffix {
    /// Byte index at which the suffix begins.
    pub index: usize,
    /// `+` or `-`, absent for `Z`.
    pub sign: Option<char>,
    /// Two-digit hour, absent for `Z`.
    pub hour: Option<String>,
    /// Whether the source used `:`.
    pub has_colon: bool,
    /// Two-digit minute when present.
    pub minute: Option<String>,
}

/// Temporal construction or conversion failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TimeError {
    /// Fractional-seconds precision was outside TiDB's accepted domain.
    InvalidFsp(FspError),
    /// One calendar field exceeded its representable or valid range.
    OutOfRange(&'static str),
    /// Calendar-to-timezone conversion failed.
    Conversion(TimeConversionError),
    /// A zero month or day is forbidden by the conversion flags.
    ZeroInDate,
    /// Month/day fields do not form an accepted MySQL date.
    InvalidDate,
    /// Hour/minute/second fields exceed MySQL's clock range.
    InvalidClock,
    /// TIMESTAMP falls outside TiDB's UTC storage range.
    TimestampOutOfRange,
    /// A temporal operation received an unsupported interval unit.
    InvalidUnit(String),
}

impl fmt::Display for TimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidFsp(error) => error.fmt(formatter),
            Self::OutOfRange(field) => write!(formatter, "time {field} is out of range"),
            Self::Conversion(error) => error.fmt(formatter),
            Self::ZeroInDate => formatter.write_str("zero month or day in date"),
            Self::InvalidDate => formatter.write_str("invalid MySQL date"),
            Self::InvalidClock => formatter.write_str("invalid MySQL clock"),
            Self::TimestampOutOfRange => formatter.write_str("timestamp is out of range"),
            Self::InvalidUnit(unit) => write!(formatter, "invalid unit {unit}"),
        }
    }
}

impl std::error::Error for TimeError {}

impl From<TimeConversionError> for TimeError {
    fn from(error: TimeConversionError) -> Self {
        Self::Conversion(error)
    }
}

/// Converts a timezone-aware value to TiDB's microsecond calendar storage.
pub fn core_time_from_datetime<TZ: TimeZone>(value: DateTime<TZ>) -> CoreTime {
    let value = value + ChronoDuration::nanoseconds(500);
    CoreTime::from_date(
        value.year() as u16,
        value.month() as u8,
        value.day() as u8,
        value.hour() as u8,
        value.minute() as u8,
        value.second() as u8,
        value.nanosecond() / 1_000,
    )
}

/// Rounds a timezone-aware datetime to TiDB's fractional precision.
pub fn round_datetime_fraction<TZ: TimeZone>(
    value: DateTime<TZ>,
    fsp: i64,
) -> Result<DateTime<TZ>, TimeError> {
    let fsp = check_fsp(fsp).map_err(TimeError::InvalidFsp)?;
    let quantum = 10_i64.pow((9 - fsp) as u32);
    let nanosecond = i64::from(value.nanosecond());
    let rounded = (nanosecond + quantum / 2) / quantum * quantum;
    value
        .checked_add_signed(ChronoDuration::nanoseconds(rounded - nanosecond))
        .ok_or(TimeError::OutOfRange("rounded value"))
}

/// Truncates a timezone-aware datetime to TiDB's fractional precision.
pub fn truncate_datetime_fraction<TZ: TimeZone>(
    value: DateTime<TZ>,
    fsp: i64,
) -> Result<DateTime<TZ>, TimeError> {
    let fsp = check_fsp(fsp).map_err(TimeError::InvalidFsp)?;
    let quantum = 10_i64.pow((9 - fsp) as u32);
    let nanosecond = i64::from(value.nanosecond());
    value
        .checked_sub_signed(ChronoDuration::nanoseconds(nanosecond % quantum))
        .ok_or(TimeError::OutOfRange("truncated value"))
}

impl Time {
    /// Constructs a temporal value from its internal calendar fields.
    pub fn new(core: CoreTime, kind: TimeType, fsp: i64) -> Result<Self, TimeError> {
        let fsp = if kind == TimeType::Date {
            0
        } else {
            check_fsp(fsp).map_err(TimeError::InvalidFsp)? as u8
        };
        Ok(Self { core, kind, fsp })
    }

    /// Constructs and bit-width-checks all calendar fields.
    #[allow(clippy::too_many_arguments)]
    pub fn from_date_checked(
        year: i32,
        month: i32,
        day: i32,
        hour: i32,
        minute: i32,
        second: i32,
        microsecond: i32,
        kind: TimeType,
        fsp: i64,
    ) -> Result<Self, TimeError> {
        for (name, value, limit) in [
            ("year", year, 1 << 14),
            ("month", month, 1 << 4),
            ("day", day, 1 << 5),
            ("hour", hour, 1 << 5),
            ("minute", minute, 1 << 6),
            ("second", second, 1 << 6),
            ("microsecond", microsecond, 1 << 20),
        ] {
            if !(0..limit).contains(&value) {
                return Err(TimeError::OutOfRange(name));
            }
        }
        Self::new(
            CoreTime::from_date(
                year as u16,
                month as u8,
                day as u8,
                hour as u8,
                minute as u8,
                second as u8,
                microsecond as u32,
            ),
            kind,
            fsp,
        )
    }

    /// Returns the internal calendar fields.
    pub const fn core_time(self) -> CoreTime {
        self.core
    }

    /// Replaces the internal calendar fields.
    pub fn set_core_time(&mut self, core: CoreTime) {
        self.core = core;
    }

    /// Returns `DATE`, `DATETIME`, or `TIMESTAMP`.
    pub const fn kind(self) -> TimeType {
        self.kind
    }

    /// Changes the temporal type; DATE forces FSP zero.
    pub fn set_kind(&mut self, kind: TimeType) {
        self.kind = kind;
        if kind == TimeType::Date {
            self.fsp = 0;
        }
    }

    /// Returns fractional-seconds precision.
    pub const fn fsp(self) -> u8 {
        self.fsp
    }

    /// Changes fractional-seconds precision; DATE remains zero.
    pub fn set_fsp(&mut self, fsp: i64) -> Result<(), TimeError> {
        if self.kind == TimeType::Date {
            return Ok(());
        }
        self.fsp = check_fsp(fsp).map_err(TimeError::InvalidFsp)? as u8;
        Ok(())
    }

    /// Returns hour, minute, and second.
    pub const fn clock(self) -> (u8, u8, u8) {
        (self.core.hour(), self.core.minute(), self.core.second())
    }

    /// Returns whether all calendar/time fields are zero.
    pub fn is_zero(self) -> bool {
        self.core == CoreTime::default()
    }

    /// Returns whether month or day is zero.
    pub const fn invalid_zero(self) -> bool {
        self.core.month() == 0 || self.core.day() == 0
    }

    /// Compares calendar fields and microseconds.
    pub fn compare(self, other: Self) -> Ordering {
        self.core.compare(other.core)
    }

    /// Parses and compares a temporal string with TiDB's maximum FSP.
    pub fn compare_string<TZ: TimeZone>(
        self,
        input: &str,
        allow_zero_in_date: bool,
        allow_invalid_date: bool,
        timezone: &TZ,
    ) -> Result<Ordering, TimeError> {
        let other = crate::parse_time(
            input,
            self.kind,
            6,
            false,
            allow_zero_in_date,
            allow_invalid_date,
            timezone,
        )?
        .time;
        Ok(self.compare(other))
    }

    /// Returns the exact packed `uint64` representation of Go `types.Time`.
    pub const fn go_raw(self) -> u64 {
        let metadata = match self.kind {
            TimeType::Date => 0b1110,
            TimeType::DateTime => (self.fsp as u64) << 1,
            TimeType::Timestamp => ((self.fsp as u64) << 1) | 1,
        };
        self.core.raw() | metadata
    }

    /// Decodes the exact packed `uint64` representation of Go `types.Time`.
    pub fn from_go_raw(raw: u64) -> Result<Self, TimeError> {
        let metadata = raw & 0b1111;
        let core = CoreTime::from_raw(raw & !0b1111);
        if metadata == 0b1110 {
            return Self::new(core, TimeType::Date, 0);
        }
        let kind = if metadata & 1 == 1 {
            TimeType::Timestamp
        } else {
            TimeType::DateTime
        };
        Self::new(core, kind, (metadata >> 1) as i64)
    }

    /// Returns the current local wall-clock value with FSP zero.
    pub fn current(kind: TimeType) -> Self {
        Self {
            core: core_time_from_datetime(Local::now()),
            kind,
            fsp: 0,
        }
    }

    /// Converts this wall-clock value between two timezone authorities.
    pub fn convert_time_zone<FromTZ: TimeZone, ToTZ: TimeZone>(
        &mut self,
        from: &FromTZ,
        to: &ToTZ,
    ) -> Result<(), TimeError> {
        if self.is_zero() {
            return Ok(());
        }
        let source = self.core.to_datetime(from)?;
        self.core = core_time_from_datetime(source.with_timezone(to));
        Ok(())
    }

    /// Converts the temporal type, adjusting a TIMESTAMP DST gap exactly once.
    ///
    /// The boolean is true when TiDB would return the adjusted value together
    /// with its DST-transition diagnostic.
    pub fn convert_kind<TZ: TimeZone>(
        self,
        kind: TimeType,
        allow_zero_in_date: bool,
        allow_invalid_date: bool,
        timezone: &TZ,
    ) -> Result<(Self, bool), TimeError> {
        let mut converted = self;
        converted.set_kind(kind);
        if self.kind == kind || self.is_zero() {
            return Ok((converted, false));
        }
        match converted.validate(allow_zero_in_date, allow_invalid_date, timezone) {
            Ok(()) => Ok((converted, false)),
            Err(TimeError::Conversion(TimeConversionError::NonexistentLocalTime))
                if kind == TimeType::Timestamp =>
            {
                converted.core =
                    core_time_from_datetime(converted.core.adjusted_datetime(timezone)?);
                converted.validate(allow_zero_in_date, allow_invalid_date, timezone)?;
                Ok((converted, true))
            }
            Err(error) => Err(error),
        }
    }

    /// Returns whether this value lies outside TiDB's temporal storage bounds.
    pub fn is_overflow<TZ: TimeZone>(self, timezone: &TZ) -> Result<bool, TimeError> {
        if self.kind == TimeType::Timestamp {
            let instant = self.core.adjusted_datetime(timezone)?.with_timezone(&Utc);
            let lower = Utc.timestamp_opt(1, 0).single().expect("valid timestamp");
            let upper = Utc
                .timestamp_opt(2_147_483_647, 999_999_000)
                .single()
                .expect("valid timestamp");
            return Ok(instant < lower || instant > upper);
        }
        let minimum = CoreTime::from_date(1, 1, 1, 0, 0, 0, 0);
        let maximum = CoreTime::from_date(9999, 12, 31, 23, 59, 59, 999_999);
        Ok(self.core.compare(minimum) == Ordering::Less
            || self.core.compare(maximum) == Ordering::Greater)
    }

    /// Serializes the exact JSON number emitted by Go `types.Time`.
    pub fn to_go_json(self) -> String {
        self.go_raw().to_string()
    }

    /// Parses the exact JSON number emitted by Go `types.Time`.
    pub fn from_go_json(input: &str) -> Result<Self, TimeError> {
        let raw = input.parse().map_err(|_| TimeError::InvalidDate)?;
        Self::from_go_raw(raw)
    }

    /// Returns TiDB's numeric DATE/DATETIME representation.
    pub fn to_number(self) -> Decimal {
        if self.is_zero() {
            return Decimal::from_int(0);
        }
        let mut text = if self.kind == TimeType::Date {
            format!(
                "{:04}{:02}{:02}",
                self.core.year(),
                self.core.month(),
                self.core.day()
            )
        } else {
            format!(
                "{:04}{:02}{:02}{:02}{:02}{:02}",
                self.core.year(),
                self.core.month(),
                self.core.day(),
                self.core.hour(),
                self.core.minute(),
                self.core.second()
            )
        };
        if self.kind != TimeType::Date && self.fsp > 0 {
            let fraction = format!("{:06}", self.core.microsecond());
            text.push('.');
            text.push_str(&fraction[..usize::from(self.fsp)]);
        }
        Decimal::from_literal(&text)
    }

    /// Converts DATE/DATETIME/TIMESTAMP clock fields to a MySQL duration.
    pub fn to_duration(self) -> Result<MySqlDuration, TimeError> {
        if self.is_zero() {
            return MySqlDuration::from_nanoseconds(0, 0).map_err(TimeError::InvalidFsp);
        }
        MySqlDuration::new(
            i64::from(self.core.hour()),
            i64::from(self.core.minute()),
            i64::from(self.core.second()),
            i64::from(self.core.microsecond()),
            i64::from(self.fsp),
        )
        .map_err(TimeError::InvalidFsp)
    }

    /// Rounds fractional seconds with TiDB's half-up rule.
    pub fn round_frac<TZ: TimeZone>(self, fsp: i64, timezone: &TZ) -> Result<Self, TimeError> {
        if self.kind == TimeType::Date || self.is_zero() {
            return Ok(self);
        }
        let fsp = check_fsp(fsp).map_err(TimeError::InvalidFsp)? as u8;
        if fsp == self.fsp {
            return Ok(self);
        }
        let quantum = 10_i64.pow(u32::from(6 - fsp));
        let microsecond = i64::from(self.core.microsecond());
        let rounded = ((microsecond + quantum / 2) / quantum) * quantum;
        let core = match self.core.to_datetime(timezone) {
            Ok(value) => {
                core_time_from_datetime(value + ChronoDuration::microseconds(rounded - microsecond))
            }
            Err(_) => {
                let clock_micros = (i64::from(self.core.hour()) * 3_600
                    + i64::from(self.core.minute()) * 60
                    + i64::from(self.core.second()))
                    * 1_000_000
                    + rounded;
                if clock_micros >= 86_400 * 1_000_000 {
                    return Err(TimeError::OutOfRange("rounded value"));
                }
                let seconds = clock_micros / 1_000_000;
                CoreTime::from_date(
                    self.core.year() as u16,
                    self.core.month(),
                    self.core.day(),
                    (seconds / 3_600) as u8,
                    (seconds % 3_600 / 60) as u8,
                    (seconds % 60) as u8,
                    (clock_micros % 1_000_000) as u32,
                )
            }
        };
        Self::new(core, self.kind, i64::from(fsp))
    }

    /// Subtracts two temporal values using instant semantics for TIMESTAMP and
    /// calendar-field semantics for DATE/DATETIME.
    pub fn sub<TZ: TimeZone>(self, other: Self, timezone: &TZ) -> Result<MySqlDuration, TimeError> {
        let nanoseconds = if self.kind == TimeType::Timestamp && other.kind == TimeType::Timestamp {
            let left = self.core.to_datetime(timezone)?;
            let right = other.core.to_datetime(timezone)?;
            let nonnegative = left >= right;
            left.signed_duration_since(right)
                .num_nanoseconds()
                .unwrap_or(if nonnegative { i64::MAX } else { i64::MIN })
        } else {
            let difference = self.core.time_diff(other.core, 1);
            let magnitude = difference
                .seconds
                .saturating_mul(1_000_000_000)
                .saturating_add(i64::from(difference.microseconds) * 1_000);
            if difference.negative {
                -magnitude
            } else {
                magnitude
            }
        };
        MySqlDuration::from_nanoseconds(nanoseconds, i64::from(self.fsp.max(other.fsp)))
            .map_err(TimeError::InvalidFsp)
    }

    /// Adds a MySQL duration to the calendar fields.
    pub fn add_duration(self, duration: MySqlDuration) -> Result<Self, TimeError> {
        let mut core = self.core.add_duration(duration.nanoseconds());
        if self.kind == TimeType::Date {
            core = CoreTime::from_date(core.year() as u16, core.month(), core.day(), 0, 0, 0, 0);
        }
        Self::new(core, self.kind, i64::from(self.fsp.max(duration.fsp())))
    }

    /// Formats this value with TiDB's MySQL `DATE_FORMAT` conversion rules.
    pub fn date_format(self, layout: &str) -> Result<String, TimeError> {
        let mut output = String::with_capacity(layout.len());
        let mut pattern = false;
        for character in layout.chars() {
            if pattern {
                self.push_date_format(character, &mut output)?;
                pattern = false;
            } else if character == '%' {
                pattern = true;
            } else {
                output.push(character);
            }
        }
        Ok(output)
    }

    fn push_date_format(self, conversion: char, output: &mut String) -> Result<(), TimeError> {
        let hour = self.core.hour();
        let minute = self.core.minute();
        let second = self.core.second();
        match conversion {
            'b' | 'M' => {
                let month = self.core.month();
                if !(1..=12).contains(&month) {
                    return Err(TimeError::InvalidDate);
                }
                let name = MONTH_NAMES[usize::from(month - 1)];
                output.push_str(if conversion == 'b' { &name[..3] } else { name });
            }
            'm' => output.push_str(&format!("{:02}", self.core.month())),
            'c' => output.push_str(&self.core.month().to_string()),
            'D' => {
                let day = self.core.day();
                output.push_str(&day.to_string());
                output.push_str(day_suffix(day));
            }
            'd' => output.push_str(&format!("{:02}", self.core.day())),
            'e' => output.push_str(&self.core.day().to_string()),
            'j' => output.push_str(&format!("{:03}", self.core.year_day())),
            'H' => output.push_str(&format!("{hour:02}")),
            'k' => output.push_str(&hour.to_string()),
            'h' | 'I' => {
                let twelve_hour = hour % 12;
                output.push_str(&format!(
                    "{:02}",
                    if twelve_hour == 0 { 12 } else { twelve_hour }
                ));
            }
            'l' => {
                let twelve_hour = hour % 12;
                output.push_str(&if twelve_hour == 0 { 12 } else { twelve_hour }.to_string());
            }
            'i' => output.push_str(&format!("{minute:02}")),
            'p' => output.push_str(if (hour / 12).is_multiple_of(2) {
                "AM"
            } else {
                "PM"
            }),
            'r' => {
                let normalized = hour % 24;
                let twelve_hour = match normalized {
                    0 | 12 => 12,
                    1..=11 => normalized,
                    _ => normalized - 12,
                };
                let meridiem = if normalized < 12 { "AM" } else { "PM" };
                output.push_str(&format!(
                    "{twelve_hour:02}:{minute:02}:{second:02} {meridiem}"
                ));
            }
            'T' => output.push_str(&format!("{hour:02}:{minute:02}:{second:02}")),
            'S' | 's' => output.push_str(&format!("{second:02}")),
            'f' => output.push_str(&format!("{:06}", self.core.microsecond())),
            'U' | 'u' | 'V' => {
                let mode = match conversion {
                    'U' => 0,
                    'u' => 1,
                    _ => 2,
                };
                output.push_str(&format!("{:02}", self.core.week(mode)));
            }
            'v' => output.push_str(&format!("{:02}", self.core.year_week(3).1)),
            'a' => output.push_str(self.core.weekday().abbreviated_name()),
            'W' => output.push_str(&self.core.weekday().to_string()),
            'w' => output.push_str(&self.core.weekday().sunday_index().to_string()),
            'X' | 'x' => {
                let mode = if conversion == 'X' { 2 } else { 3 };
                let year = self.core.year_week(mode).0;
                if year < 0 {
                    output.push_str(&u32::MAX.to_string());
                } else {
                    output.push_str(&format!("{year:04}"));
                }
            }
            'Y' => output.push_str(&format!("{:04}", self.core.year())),
            'y' => output.push_str(&format!("{:04}", self.core.year())[2..]),
            _ => output.push(conversion),
        }
        Ok(())
    }

    /// Validates DATE/DATETIME/TIMESTAMP using TiDB's conversion flags.
    pub fn validate<TZ: TimeZone>(
        self,
        allow_zero_in_date: bool,
        allow_invalid_date: bool,
        timezone: &TZ,
    ) -> Result<(), TimeError> {
        if self.kind == TimeType::Timestamp {
            if self.is_zero() {
                return Ok(());
            }
            let utc = self.core.to_datetime(timezone)?.with_timezone(&Utc);
            let seconds = utc.timestamp();
            if !(1..=2_147_483_647).contains(&seconds) {
                return Err(TimeError::TimestampOutOfRange);
            }
            return Ok(());
        }

        let year = self.core.year();
        let month = self.core.month();
        let day = self.core.day();
        if year == 0 && month == 0 && day == 0 {
            return self.validate_clock();
        }
        if !allow_zero_in_date && (month == 0 || day == 0) {
            return Err(TimeError::ZeroInDate);
        }
        if year > 9999 || month > 12 {
            return Err(TimeError::InvalidDate);
        }
        let maximum_day = if allow_invalid_date || month == 0 {
            31
        } else {
            get_last_day(year, month)
        };
        if day > maximum_day {
            return Err(TimeError::InvalidDate);
        }
        self.validate_clock()
    }

    fn validate_clock(self) -> Result<(), TimeError> {
        if self.core.hour() >= 24 || self.core.minute() >= 60 || self.core.second() >= 60 {
            Err(TimeError::InvalidClock)
        } else {
            Ok(())
        }
    }

    /// Encodes TiDB's packed temporal storage representation.
    pub fn to_packed_uint(self) -> Result<u64, TimeError> {
        PackedTime::from_parts(
            self.core.year() as u16,
            self.core.month(),
            self.core.day(),
            self.core.hour(),
            self.core.minute(),
            self.core.second(),
            self.core.microsecond(),
        )
        .map(PackedTime::raw)
        .map_err(|_| TimeError::OutOfRange("packed value"))
    }

    /// Decodes TiDB's packed temporal storage representation.
    pub fn from_packed_uint(packed: u64, kind: TimeType, fsp: i64) -> Result<Self, TimeError> {
        let parts = PackedTime::from_raw(packed).parts();
        Self::from_date_checked(
            i32::from(parts.year),
            i32::from(parts.month),
            i32::from(parts.day),
            i32::from(parts.hour),
            i32::from(parts.minute),
            i32::from(parts.second),
            parts.microsecond as i32,
            kind,
            fsp,
        )
    }
}

const fn day_suffix(day: u8) -> &'static str {
    match day {
        1 | 21 | 31 => "st",
        2 | 22 => "nd",
        3 | 23 => "rd",
        _ => "th",
    }
}

impl fmt::Display for Time {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:04}-{:02}-{:02}",
            self.core.year(),
            self.core.month(),
            self.core.day()
        )?;
        if self.kind == TimeType::Date {
            return Ok(());
        }
        write!(
            formatter,
            " {:02}:{:02}:{:02}",
            self.core.hour(),
            self.core.minute(),
            self.core.second()
        )?;
        if self.fsp > 0 {
            let fraction = format!("{:06}", self.core.microsecond());
            write!(formatter, ".{}", &fraction[..usize::from(self.fsp)])?;
        }
        Ok(())
    }
}

/// Returns the number of fractional digits in a temporal literal, capped at 6.
pub fn get_fsp(value: &str) -> u8 {
    let index = get_frac_index(value);
    if index < 0 {
        return 0;
    }
    value.as_bytes()[index as usize + 1..]
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count()
        .min(6) as u8
}

/// Returns the byte index of the fraction dot, or `-1`.
pub fn get_frac_index(value: &str) -> isize {
    let bytes = value.as_bytes();
    let end = get_timezone(value).map_or(bytes.len(), |timezone| timezone.index);
    for index in (0..end).rev() {
        let byte = bytes[index];
        if byte != b'+' && byte != b'-' && is_ascii_punctuation(byte) {
            return if byte == b'.' { index as isize } else { -1 };
        }
    }
    -1
}

/// Parses TiDB's supported trailing `Z`, `+HH`, `+HHMM`, and `+HH:MM` forms.
pub fn get_timezone(value: &str) -> Option<TimezoneSuffix> {
    let bytes = value.as_bytes();
    if bytes.last() == Some(&b'Z') {
        return Some(TimezoneSuffix {
            index: bytes.len() - 1,
            sign: None,
            hour: None,
            has_colon: false,
            minute: None,
        });
    }

    for suffix_length in [6_usize, 5, 3] {
        if bytes.len() < suffix_length {
            continue;
        }
        let index = bytes.len() - suffix_length;
        let sign = match bytes[index] {
            b'+' => '+',
            b'-' => '-',
            _ => continue,
        };
        let suffix = &bytes[index + 1..];
        let (hour, has_colon, minute) = match suffix_length {
            3 if suffix.iter().all(u8::is_ascii_digit) => (&suffix[..2], false, None),
            5 if suffix.iter().all(u8::is_ascii_digit) => {
                (&suffix[..2], false, Some(&suffix[2..4]))
            }
            6 if suffix[2] == b':'
                && suffix[..2].iter().all(u8::is_ascii_digit)
                && suffix[3..].iter().all(u8::is_ascii_digit) =>
            {
                (&suffix[..2], true, Some(&suffix[3..5]))
            }
            _ => continue,
        };
        return Some(TimezoneSuffix {
            index,
            sign: Some(sign),
            hour: Some(String::from_utf8(hour.to_vec()).expect("ASCII digits")),
            has_colon,
            minute: minute
                .map(|minute| String::from_utf8(minute.to_vec()).expect("ASCII timezone minute")),
        });
    }
    None
}

const fn is_ascii_punctuation(byte: u8) -> bool {
    matches!(byte, 0x21..=0x2f | 0x3a..=0x40 | 0x5b..=0x60 | 0x7b..=0x7e)
}

/// Returns the uncapped suffix length after the last decimal point.
pub fn date_fsp(value: &str) -> usize {
    value
        .rsplit_once('.')
        .map_or(0, |(_, fraction)| fraction.len())
}

/// Formats an integer with at least `width` decimal digits.
pub fn format_int_width(value: i32, width: usize) -> String {
    format!("{value:0width$}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_time_encoding() {
        let time =
            Time::from_date_checked(2012, 12, 31, 11, 30, 45, 123_456, TimeType::DateTime, 6)
                .unwrap();
        let packed = time.to_packed_uint().unwrap();
        assert_eq!(
            Time::from_packed_uint(packed, TimeType::DateTime, 6).unwrap(),
            time
        );
    }

    #[test]
    fn test_go_time_encoding_source_rows() {
        for (core, kind, fsp, expected) in [
            (
                CoreTime::from_date(2019, 9, 16, 0, 0, 0, 0),
                TimeType::DateTime,
                0,
                0b1111110001110011000000000000000000000000000000000000000000000,
            ),
            (
                CoreTime::from_date(2019, 12, 31, 23, 59, 59, 999_999),
                TimeType::Timestamp,
                3,
                0b1111110001111001111110111111011111011111101000010001111110111,
            ),
            (
                CoreTime::from_date(2020, 1, 5, 0, 0, 0, 0),
                TimeType::Date,
                0,
                0b1111110010000010010100000000000000000000000000000000000001110,
            ),
        ] {
            let time = Time::new(core, kind, fsp).unwrap();
            assert_eq!(time.go_raw(), expected);
            assert_eq!(Time::from_go_raw(expected).unwrap(), time);
        }
    }

    #[test]
    fn test_compare_string_source_rows() {
        for (left, right, expected) in [
            (
                "2011-10-10 11:11:11",
                "2011-10-10 11:11:11",
                Ordering::Equal,
            ),
            (
                "2011-10-10 11:11:11.123456",
                "2011-10-10 11:11:11.1",
                Ordering::Greater,
            ),
            (
                "2011-10-10 11:11:11",
                "2011-10-10 11:11:11.123",
                Ordering::Less,
            ),
            ("0000-00-00 00:00:00", "2011-10-10 11:11:11", Ordering::Less),
            (
                "0000-00-00 00:00:00",
                "0000-00-00 00:00:00",
                Ordering::Equal,
            ),
        ] {
            let left = crate::parse_time(
                left,
                TimeType::DateTime,
                6,
                false,
                true,
                false,
                &chrono_tz::UTC,
            )
            .unwrap()
            .time;
            assert_eq!(
                left.compare_string(right, true, false, &chrono_tz::UTC)
                    .unwrap(),
                expected
            );
        }
        let value = Time::new(
            CoreTime::from_date(2011, 10, 10, 11, 11, 11, 0),
            TimeType::DateTime,
            6,
        )
        .unwrap();
        assert!(value
            .compare_string("Test should error", true, false, &chrono_tz::UTC)
            .is_err());
    }

    #[test]
    fn test_convert_kind_dst_gap_and_overflow_source_rows() {
        let los_angeles: chrono_tz::Tz = "America/Los_Angeles".parse().unwrap();
        let datetime = Time::new(
            CoreTime::from_date(2018, 3, 11, 2, 0, 16, 0),
            TimeType::DateTime,
            0,
        )
        .unwrap();
        let (timestamp, adjusted) = datetime
            .convert_kind(TimeType::Timestamp, false, false, &los_angeles)
            .unwrap();
        assert!(adjusted);
        assert_eq!(timestamp.to_string(), "2018-03-11 03:00:00");

        for (core, overflow) in [
            (CoreTime::from_date(2012, 12, 31, 11, 30, 45, 0), false),
            (CoreTime::from_date(999, 12, 31, 22, 0, 0, 0), false),
            (CoreTime::from_date(9999, 12, 31, 23, 59, 59, 0), false),
            (CoreTime::from_date(1, 1, 1, 0, 0, 0, 0), false),
            (CoreTime::from_date(0, 1, 1, 0, 0, 0, 0), true),
        ] {
            let value = Time::new(core, TimeType::DateTime, 0).unwrap();
            assert_eq!(value.is_overflow(&chrono_tz::UTC).unwrap(), overflow);
        }
    }

    #[test]
    fn test_go_json_round_trip_source_row() {
        let original = Time::new(
            CoreTime::from_date(2017, 1, 18, 1, 1, 1, 123_456),
            TimeType::DateTime,
            6,
        )
        .unwrap();
        let json = original.to_go_json();
        assert_eq!(Time::from_go_json(&json).unwrap(), original);
        assert!(Time::from_go_json("\"invalid\"").is_err());
    }

    #[test]
    fn test_date_time_and_type_fsp() {
        let core = CoreTime::from_date(2012, 12, 12, 10, 10, 10, 123_456);
        let mut datetime = Time::new(core, TimeType::DateTime, 3).unwrap();
        assert_eq!(datetime.to_string(), "2012-12-12 10:10:10.123");
        datetime.set_kind(TimeType::Timestamp);
        assert_eq!(datetime.kind(), TimeType::Timestamp);
        datetime.set_kind(TimeType::Date);
        assert_eq!(datetime.to_string(), "2012-12-12");
        assert_eq!(datetime.fsp(), 0);
        datetime.set_fsp(6).unwrap();
        assert_eq!(datetime.fsp(), 0);
    }

    #[test]
    fn test_date_format_source_rows() {
        let full_layout =
            "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y %%";
        for (core, layout, expected) in [
            (
                CoreTime::from_date(2010, 1, 7, 23, 12, 34, 123_450),
                full_layout,
                "Jan January 01 1 7th 07 7 007 23 11 12 PM 11:12:34 PM 23:12:34 34 123450 01 01 01 01 Thu Thursday 4 2010 2010 2010 10 %",
            ),
            (
                CoreTime::from_date(2012, 12, 21, 23, 12, 34, 123_456),
                full_layout,
                "Dec December 12 12 21st 21 21 356 23 11 12 PM 11:12:34 PM 23:12:34 34 123456 51 51 51 51 Fri Friday 5 2012 2012 2012 12 %",
            ),
            (
                CoreTime::from_date(0, 1, 1, 0, 0, 0, 123_456),
                "%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %v %Y %y %%",
                "Jan January 01 1 1st 01 1 001 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 52 0000 00 %",
            ),
            (
                CoreTime::from_date(2016, 9, 3, 0, 59, 59, 123_456),
                "abc%b %M %m %c %D %d %e %j %k %h %i %p %r %T %s %f %U %u %V %v %a %W %w %X %x %Y %y!123 %%xyz %z",
                "abcSep September 09 9 3rd 03 3 247 0 12 59 AM 12:59:59 AM 00:59:59 59 123456 35 35 35 35 Sat Saturday 6 2016 2016 2016 16!123 %xyz z",
            ),
            (
                CoreTime::from_date(2012, 10, 1, 0, 0, 0, 0),
                "%b %M %m %c %D %d %e %j %k %H %i %p %r %T %s %f %v %x %Y %y %%",
                "Oct October 10 10 1st 01 1 275 0 00 00 AM 12:00:00 AM 00:00:00 00 000000 40 2012 2012 12 %",
            ),
            (
                CoreTime::from_date(0, 1, 0, 0, 0, 0, 123_456),
                full_layout,
                "Jan January 01 1 0th 00 0 000 0 12 00 AM 12:00:00 AM 00:00:00 00 123456 00 00 00 52 Fri Friday 5 4294967295 4294967295 0000 00 %",
            ),
        ] {
            let time = Time::new(core, TimeType::DateTime, 6).unwrap();
            assert_eq!(time.date_format(layout).unwrap(), expected);
        }

        let invalid_month = Time::new(
            CoreTime::from_date(2010, 0, 1, 0, 0, 0, 0),
            TimeType::DateTime,
            0,
        )
        .unwrap();
        assert_eq!(invalid_month.date_format("%b"), Err(TimeError::InvalidDate));
        assert_eq!(invalid_month.date_format("%M"), Err(TimeError::InvalidDate));
        assert_eq!(invalid_month.date_format("trailing%").unwrap(), "trailing");
    }

    #[test]
    fn test_get_fsp_and_frac_index() {
        for (value, index, fsp) in [
            ("2012-01-01 00:00:00", -1, 0),
            ("2012-01-01 00:00:00.1", 19, 1),
            ("00:00:00.1234567", 8, 6),
            ("1.2e3", 1, 1),
            ("2019.01.01 00:00:00", -1, 0),
            ("2019.01.01 00:00:00.1", 19, 1),
            ("12345.6", 5, 1),
            ("2020-01-01 12:00:00.123456 +0600 PST", 19, 6),
            ("2020-01-01 12:00:00.123456 -0600 PST", 19, 6),
        ] {
            assert_eq!(get_frac_index(value), index);
            assert_eq!(get_fsp(value), fsp);
        }
    }

    #[test]
    fn test_get_timezone_source_rows() {
        for (input, expected) in [
            ("2020-10-10T10:10:10Z", Some((19, None, None, false, None))),
            ("2020-10-10T10:10:10", None),
            (
                "2020-10-10T10:10:10-08",
                Some((19, Some('-'), Some("08"), false, None)),
            ),
            (
                "2020-10-10T10:10:10-0700",
                Some((19, Some('-'), Some("07"), false, Some("00"))),
            ),
            (
                "2020-10-10T10:10:10+08:20",
                Some((19, Some('+'), Some("08"), true, Some("20"))),
            ),
            (
                "2020-10-10T10:10:10+08:10",
                Some((19, Some('+'), Some("08"), true, Some("10"))),
            ),
            ("2020-10-10T10:10:10+8:00", None),
            ("2020-10-10T10:10:10+082:10", None),
            ("2020-10-10T10:10:10+08:101", None),
            ("2020-10-10T10:10:10+T8:11", None),
            (
                "2020-09-06T05:49:13.293Z",
                Some((23, None, None, false, None)),
            ),
            ("2020-09-06T05:49:13.293", None),
        ] {
            let actual = get_timezone(input);
            let actual = actual.as_ref().map(|timezone| {
                (
                    timezone.index,
                    timezone.sign,
                    timezone.hour.as_deref(),
                    timezone.has_colon,
                    timezone.minute.as_deref(),
                )
            });
            assert_eq!(actual, expected, "{input}");
        }
    }

    #[test]
    fn test_invalid_zero_and_format_int_width_n() {
        assert!(Time::new(CoreTime::default(), TimeType::DateTime, 0)
            .unwrap()
            .is_zero());
        assert!(Time::new(
            CoreTime::from_date(2020, 0, 1, 0, 0, 0, 0),
            TimeType::Date,
            0
        )
        .unwrap()
        .invalid_zero());
        assert_eq!(format_int_width(12, 4), "0012");
        assert_eq!(format_int_width(12345, 4), "12345");
    }

    #[test]
    fn test_from_datetime_rounds_to_microseconds_like_source() {
        for (input, expected) in [
            (
                "2006-01-02T15:04:05.999999999Z",
                CoreTime::from_date(2006, 1, 2, 15, 4, 6, 0),
            ),
            (
                "2006-01-02T15:04:05.999999000Z",
                CoreTime::from_date(2006, 1, 2, 15, 4, 5, 999_999),
            ),
            (
                "2006-01-02T15:04:05.999999499Z",
                CoreTime::from_date(2006, 1, 2, 15, 4, 5, 999_999),
            ),
            (
                "2006-01-02T15:04:05.999999500Z",
                CoreTime::from_date(2006, 1, 2, 15, 4, 6, 0),
            ),
            (
                "2006-01-02T15:04:05.000000501Z",
                CoreTime::from_date(2006, 1, 2, 15, 4, 5, 1),
            ),
        ] {
            let value = DateTime::parse_from_rfc3339(input).unwrap();
            assert_eq!(core_time_from_datetime(value), expected, "{input}");
        }
    }

    #[test]
    fn test_convert_time_zone_source_rows() {
        let utc = chrono_tz::UTC;
        let shanghai: chrono_tz::Tz = "Asia/Shanghai".parse().unwrap();
        for (input, from_shanghai, expected) in [
            (
                CoreTime::from_date(2017, 1, 1, 0, 0, 0, 0),
                false,
                CoreTime::from_date(2017, 1, 1, 8, 0, 0, 0),
            ),
            (
                CoreTime::from_date(2017, 1, 1, 8, 0, 0, 0),
                true,
                CoreTime::from_date(2017, 1, 1, 0, 0, 0, 0),
            ),
        ] {
            let mut value = Time::new(input, TimeType::DateTime, 0).unwrap();
            if from_shanghai {
                value.convert_time_zone(&shanghai, &utc).unwrap();
            } else {
                value.convert_time_zone(&utc, &shanghai).unwrap();
            }
            assert_eq!(value.core_time(), expected);
        }

        let mut zero = Time::new(CoreTime::default(), TimeType::DateTime, 0).unwrap();
        zero.convert_time_zone(&shanghai, &utc).unwrap();
        assert!(zero.is_zero());
    }

    #[test]
    fn test_current_time_preserves_requested_type_and_zero_fsp() {
        let value = Time::current(TimeType::Timestamp);
        assert_eq!(value.kind(), TimeType::Timestamp);
        assert_eq!(value.fsp(), 0);
    }

    #[test]
    fn test_to_number_and_duration_source_shapes() {
        for (time, expected) in [
            (
                Time::from_date_checked(2012, 12, 31, 11, 30, 45, 0, TimeType::DateTime, 0)
                    .unwrap(),
                "20121231113045",
            ),
            (
                Time::from_date_checked(2012, 12, 31, 11, 30, 45, 123_345, TimeType::DateTime, 3)
                    .unwrap(),
                "20121231113045.123",
            ),
            (
                Time::from_date_checked(2012, 12, 31, 11, 30, 45, 123_345, TimeType::Date, 0)
                    .unwrap(),
                "20121231",
            ),
        ] {
            assert_eq!(time.to_number().to_string(), expected);
        }
        assert_eq!(
            Time::new(CoreTime::default(), TimeType::DateTime, 0)
                .unwrap()
                .to_number()
                .to_string(),
            "0"
        );

        let time =
            Time::from_date_checked(2012, 12, 12, 10, 10, 10, 123_456, TimeType::DateTime, 6)
                .unwrap();
        assert_eq!(time.to_duration().unwrap().to_string(), "10:10:10.123456");
        assert_eq!(
            Time::new(CoreTime::default(), TimeType::DateTime, 6)
                .unwrap()
                .to_duration()
                .unwrap()
                .to_string(),
            "00:00:00"
        );
    }

    #[test]
    fn test_round_frac_source_rows() {
        let timezone = chrono_tz::UTC;
        for (core, fsp, expected) in [
            (
                CoreTime::from_date(2012, 12, 31, 11, 30, 45, 123_456),
                4,
                "2012-12-31 11:30:45.1235",
            ),
            (
                CoreTime::from_date(2012, 12, 31, 11, 30, 45, 123_456),
                6,
                "2012-12-31 11:30:45.123456",
            ),
            (
                CoreTime::from_date(2012, 12, 31, 11, 30, 45, 123_456),
                0,
                "2012-12-31 11:30:45",
            ),
            (
                CoreTime::from_date(2012, 12, 31, 11, 30, 45, 123_456),
                1,
                "2012-12-31 11:30:45.1",
            ),
            (
                CoreTime::from_date(2012, 12, 31, 11, 30, 45, 999_999),
                4,
                "2012-12-31 11:30:46.0000",
            ),
            (
                CoreTime::from_date(2012, 12, 31, 11, 30, 45, 999_999),
                0,
                "2012-12-31 11:30:46",
            ),
            (
                CoreTime::from_date(2012, 0, 0, 11, 30, 45, 999_999),
                3,
                "2012-00-00 11:30:46.000",
            ),
            (
                CoreTime::from_date(2011, 11, 11, 10, 10, 10, 888_888),
                0,
                "2011-11-11 10:10:11",
            ),
            (
                CoreTime::from_date(2011, 11, 11, 10, 10, 10, 111_111),
                0,
                "2011-11-11 10:10:10",
            ),
        ] {
            let time = Time::new(core, TimeType::DateTime, 6).unwrap();
            assert_eq!(
                time.round_frac(fsp, &timezone).unwrap().to_string(),
                expected
            );
        }
    }

    #[test]
    fn test_standalone_round_and_truncate_frac_source_rows() {
        let first = Utc
            .with_ymd_and_hms(2011, 11, 11, 10, 10, 10)
            .unwrap()
            .with_nanosecond(888_888)
            .unwrap();
        let second = Utc
            .with_ymd_and_hms(2011, 11, 11, 10, 10, 10)
            .unwrap()
            .with_nanosecond(111_111)
            .unwrap();
        assert_eq!(round_datetime_fraction(first, 0).unwrap().second(), 10);
        assert_eq!(round_datetime_fraction(second, 0).unwrap().second(), 10);
        assert_eq!(truncate_datetime_fraction(first, 0).unwrap().second(), 10);
        assert_eq!(truncate_datetime_fraction(second, 0).unwrap().second(), 10);
        assert_eq!(date_fsp("2004-01-01 12:00:00.1111111"), 7);
    }

    #[test]
    fn test_time_add_and_sub_source_rows() {
        let timezone = chrono_tz::UTC;
        for (left, right, expected) in [
            (
                CoreTime::from_date(2017, 1, 18, 1, 1, 1, 0),
                CoreTime::from_date(2017, 1, 18, 0, 0, 1, 0),
                "01:01:00.000000",
            ),
            (
                CoreTime::from_date(2017, 1, 18, 1, 1, 1, 0),
                CoreTime::from_date(2017, 1, 18, 1, 1, 1, 0),
                "00:00:00.000000",
            ),
            (
                CoreTime::from_date(2019, 4, 12, 18, 20, 0, 0),
                CoreTime::from_date(2019, 4, 12, 14, 0, 0, 0),
                "04:20:00.000000",
            ),
        ] {
            let left = Time::new(left, TimeType::DateTime, 6).unwrap();
            let right = Time::new(right, TimeType::DateTime, 6).unwrap();
            assert_eq!(left.sub(right, &timezone).unwrap().to_string(), expected);
        }

        for (time, duration, expected) in [
            (
                CoreTime::from_date(2017, 1, 18, 0, 0, 0, 0),
                MySqlDuration::new(12, 30, 59, 0, 0).unwrap(),
                "2017-01-18 12:30:59",
            ),
            (
                CoreTime::from_date(2017, 1, 18, 1, 1, 1, 0),
                MySqlDuration::new(12, 30, 59, 0, 0).unwrap(),
                "2017-01-18 13:32:00",
            ),
            (
                CoreTime::from_date(2017, 1, 18, 1, 1, 1, 123_457),
                MySqlDuration::new(12, 30, 59, 0, 6).unwrap(),
                "2017-01-18 13:32:00.123457",
            ),
            (
                CoreTime::from_date(2017, 1, 18, 1, 1, 1, 0),
                MySqlDuration::new(838, 59, 59, 0, 0).unwrap(),
                "2017-02-22 00:01:00",
            ),
            (
                CoreTime::from_date(2017, 8, 21, 15, 34, 42, 0),
                MySqlDuration::new(-838, -59, -59, 0, 0).unwrap(),
                "2017-07-17 16:34:43",
            ),
            (
                CoreTime::from_date(2017, 8, 21, 0, 0, 0, 0),
                MySqlDuration::new(1, 1, 1, 1_000, 3).unwrap(),
                "2017-08-21 01:01:01.001",
            ),
        ] {
            let time = Time::new(time, TimeType::DateTime, duration.fsp().into()).unwrap();
            assert_eq!(time.add_duration(duration).unwrap().to_string(), expected);
        }
    }

    #[test]
    fn test_validate_month_day_source_rows() {
        for (core, valid) in [
            (CoreTime::from_date(1900, 2, 29, 0, 0, 0, 0), false),
            (CoreTime::from_date(1900, 2, 28, 0, 0, 0, 0), true),
            (CoreTime::from_date(2000, 2, 29, 0, 0, 0, 0), true),
            (CoreTime::from_date(2000, 1, 1, 0, 0, 0, 0), true),
            (CoreTime::from_date(1900, 1, 1, 0, 0, 0, 0), true),
            (CoreTime::from_date(1900, 1, 31, 0, 0, 0, 0), true),
            (CoreTime::from_date(1900, 4, 1, 0, 0, 0, 0), true),
            (CoreTime::from_date(1900, 4, 31, 0, 0, 0, 0), false),
            (CoreTime::from_date(1900, 4, 30, 0, 0, 0, 0), true),
            (CoreTime::from_date(2000, 2, 30, 0, 0, 0, 0), false),
            (CoreTime::from_date(2000, 13, 1, 0, 0, 0, 0), false),
            (CoreTime::from_date(4000, 2, 29, 0, 0, 0, 0), true),
            (CoreTime::from_date(3200, 2, 29, 0, 0, 0, 0), true),
        ] {
            let value = Time::new(core, TimeType::Date, 0).unwrap();
            assert_eq!(
                value.validate(false, false, &chrono_tz::UTC).is_ok(),
                valid,
                "{core}"
            );
        }

        let invalid_leap = Time::new(
            CoreTime::from_date(1900, 2, 29, 0, 0, 0, 0),
            TimeType::Date,
            0,
        )
        .unwrap();
        assert!(invalid_leap.validate(false, true, &chrono_tz::UTC).is_ok());
        let zero_in_date = Time::new(
            CoreTime::from_date(2020, 0, 1, 0, 0, 0, 0),
            TimeType::Date,
            0,
        )
        .unwrap();
        assert_eq!(
            zero_in_date.validate(false, false, &chrono_tz::UTC),
            Err(TimeError::ZeroInDate)
        );
        assert!(zero_in_date.validate(true, false, &chrono_tz::UTC).is_ok());
    }

    #[test]
    fn test_validate_timestamp_source_bounds_and_dst_rows() {
        let shanghai: chrono_tz::Tz = "Asia/Shanghai".parse().unwrap();
        let los_angeles: chrono_tz::Tz = "America/Los_Angeles".parse().unwrap();
        let london: chrono_tz::Tz = "Europe/London".parse().unwrap();
        for (timezone, core, valid) in [
            (
                shanghai,
                CoreTime::from_date(2038, 1, 19, 11, 14, 7, 0),
                true,
            ),
            (shanghai, CoreTime::from_date(1970, 1, 1, 8, 1, 1, 0), true),
            (
                shanghai,
                CoreTime::from_date(2038, 1, 19, 12, 14, 7, 0),
                false,
            ),
            (shanghai, CoreTime::from_date(1970, 1, 1, 7, 1, 1, 0), false),
            (
                chrono_tz::UTC,
                CoreTime::from_date(2038, 1, 19, 3, 14, 7, 0),
                true,
            ),
            (
                chrono_tz::UTC,
                CoreTime::from_date(1970, 1, 1, 0, 1, 1, 0),
                true,
            ),
            (
                chrono_tz::UTC,
                CoreTime::from_date(2038, 1, 19, 4, 14, 7, 0),
                false,
            ),
            (
                chrono_tz::UTC,
                CoreTime::from_date(1969, 1, 1, 0, 0, 0, 0),
                false,
            ),
            (
                los_angeles,
                CoreTime::from_date(2018, 3, 11, 1, 0, 50, 0),
                true,
            ),
            (
                los_angeles,
                CoreTime::from_date(2018, 3, 11, 2, 0, 16, 0),
                false,
            ),
            (
                los_angeles,
                CoreTime::from_date(2018, 3, 11, 3, 0, 20, 0),
                true,
            ),
            (
                shanghai,
                CoreTime::from_date(2018, 3, 11, 1, 0, 50, 0),
                true,
            ),
            (
                shanghai,
                CoreTime::from_date(2018, 3, 11, 2, 0, 16, 0),
                true,
            ),
            (
                shanghai,
                CoreTime::from_date(2018, 3, 11, 3, 0, 20, 0),
                true,
            ),
            (london, CoreTime::from_date(2019, 3, 31, 0, 0, 20, 0), true),
            (london, CoreTime::from_date(2019, 3, 31, 1, 0, 20, 0), false),
            (london, CoreTime::from_date(2019, 3, 31, 2, 0, 20, 0), true),
        ] {
            let value = Time::new(core, TimeType::Timestamp, 0).unwrap();
            assert_eq!(
                value.validate(false, false, &timezone).is_ok(),
                valid,
                "{timezone} {core}"
            );
        }
        assert!(Time::new(CoreTime::default(), TimeType::Timestamp, 0)
            .unwrap()
            .validate(false, false, &chrono_tz::UTC)
            .is_ok());
    }
}
