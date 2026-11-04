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

//! The source-defined packed representation shared by TiDB temporal codecs.
//!
//! Go `types.Time::ToPackedUint` deliberately separates the packed calendar
//! bits from the field type (`DATE`, `DATETIME`, or `TIMESTAMP`) and fractional
//! precision.  The field metadata travels beside the value in a schema; it is
//! not part of the eight-byte payload.  This leaf therefore models only the
//! lossless packed payload and does not parse or format SQL temporal text.

/// An opaque MySQL calendar value in TiDB's eight-byte packed representation.
///
/// The raw value is ordered lexicographically when encoded big-endian, exactly
/// like Go `codec.EncodeUint`.  `PackedTime` intentionally does not carry a
/// temporal field type or timezone; those are caller-owned schema/context
/// concerns at the same boundary as Go's `Time.ToPackedUint`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PackedTime(u64);

impl PackedTime {
    /// The all-zero packed representation used by Go's zero `Time`.
    pub const ZERO: Self = Self(0);

    /// Wraps the raw bits returned by Go `Time::ToPackedUint`.
    ///
    /// Go `FromPackedUint` performs bit extraction without calendar validation;
    /// keeping this constructor infallible preserves that byte-level contract.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the exact packed bits.
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Returns whether this is Go's zero temporal payload.
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Packs calendar fields using `types.Time::ToPackedUint`'s bit layout.
    ///
    /// This constructor checks only the representable bit widths.  Calendar
    /// validity (for example, April 31) belongs to the temporal type checker,
    /// not this storage boundary.  The error carries the field name so a
    /// future statement context can choose strict/warn behavior without this
    /// type guessing at policy.
    pub fn from_parts(
        year: u16,
        month: u8,
        day: u8,
        hour: u8,
        minute: u8,
        second: u8,
        microsecond: u32,
    ) -> Result<Self, PackedTimeError> {
        if year > 9_999 {
            return Err(PackedTimeError::OutOfRange("year"));
        }
        if month > 12 {
            return Err(PackedTimeError::OutOfRange("month"));
        }
        if day > 31 {
            return Err(PackedTimeError::OutOfRange("day"));
        }
        if hour > 23 {
            return Err(PackedTimeError::OutOfRange("hour"));
        }
        if minute > 59 {
            return Err(PackedTimeError::OutOfRange("minute"));
        }
        if second > 59 {
            return Err(PackedTimeError::OutOfRange("second"));
        }
        if microsecond > 999_999 {
            return Err(PackedTimeError::OutOfRange("microsecond"));
        }

        let ymd = (u64::from(year) * 13 + u64::from(month)) << 5 | u64::from(day);
        let hms = u64::from(hour) << 12 | u64::from(minute) << 6 | u64::from(second);
        Ok(Self((ymd << 17 | hms) << 24 | u64::from(microsecond)))
    }

    /// Decodes the bit fields used by Go `Time::FromPackedUint`.
    pub const fn parts(self) -> PackedTimeParts {
        let ymdhms = self.0 >> 24;
        let ymd = ymdhms >> 17;
        let day = (ymd & 0x1f) as u8;
        let ym = ymd >> 5;
        let month = (ym % 13) as u8;
        let year = (ym / 13) as u16;
        let hms = ymdhms & 0x1ffff;
        let second = (hms & 0x3f) as u8;
        let minute = ((hms >> 6) & 0x3f) as u8;
        let hour = (hms >> 12) as u8;
        let microsecond = (self.0 & 0x00ff_ffff) as u32;
        PackedTimeParts {
            year,
            month,
            day,
            hour,
            minute,
            second,
            microsecond,
        }
    }
}

/// Calendar fields extracted from a [`PackedTime`] payload.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PackedTimeParts {
    /// Calendar year (0–9999 in the packed bit layout).
    pub year: u16,
    /// Calendar month (0–12 in the packed bit layout).
    pub month: u8,
    /// Calendar day (0–31 in the packed bit layout).
    pub day: u8,
    /// Hour (0–31 in the five-bit field; valid MySQL times use 0–23).
    pub hour: u8,
    /// Minute (0–63 in the six-bit field; valid MySQL times use 0–59).
    pub minute: u8,
    /// Second (0–63 in the six-bit field; valid MySQL times use 0–59).
    pub second: u8,
    /// Microseconds (a 24-bit payload; valid MySQL values use 0–999999).
    pub microsecond: u32,
}

/// An impossible bit-width for [`PackedTime::from_parts`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum PackedTimeError {
    /// The named field does not fit its source-defined packed range.
    OutOfRange(&'static str),
}

impl std::fmt::Display for PackedTimeError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::OutOfRange(field) => write!(formatter, "packed temporal {field} is out of range"),
        }
    }
}

impl std::error::Error for PackedTimeError {}
