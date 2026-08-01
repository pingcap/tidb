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

//! The session's `time_zone` as a value the temporal code can convert with:
//! the Rust shape of the `*time.Location` Go threads out of
//! `SessionVars.Location()` (`pkg/sessionctx/variable/session.go`) and into
//! `types.Context` (`pkg/types/context.go`).
//!
//! # Why it lives here and implements `chrono::TimeZone`
//!
//! Go has exactly ONE zone type. `time.FixedZone("+08:00", 8*3600)` and
//! `time.LoadLocation("America/Los_Angeles")` are both `*time.Location`, so
//! every function that takes a zone -- `Time.ConvertTimeZone`,
//! `tablecodec.flatten`/`unflatten`, `codec.EncodeKey`, `ParseTime` --
//! takes the same parameter and needs no case analysis.
//!
//! Rust's `chrono` splits the two: a fixed offset is `FixedOffset` and an
//! IANA zone is `chrono_tz::Tz`, and they are distinct types. Matching on
//! the pair at each call site would put a two-arm `match` in front of every
//! conversion in the engine -- and, worse, would let a call site silently
//! handle only one arm. Implementing [`TimeZone`] for the union once
//! restores Go's shape: there is one zone type, it goes anywhere a zone
//! goes, and the DST-aware arm cannot be forgotten.
//!
//! The type sits in `tidb-datatype` rather than beside the session because
//! the storage codecs (`tidb-codec`, `tidb-tablecodec`) are the code that
//! needs it most and they are BELOW the session in the crate graph, exactly
//! as Go's `tablecodec` is below `sessionctx`.

use chrono::{FixedOffset, LocalResult, NaiveDate, NaiveDateTime, Offset, TimeZone};
use chrono_tz::Tz;

/// The session `time_zone`: a fixed offset (Go `time.FixedZone`) or a named
/// IANA zone (Go `time.LoadLocation`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionTimeZone {
    /// A fixed offset east of UTC with its display name.
    Fixed {
        /// The zone's display name.
        name: String,
        /// Seconds east of UTC.
        offset_secs: i32,
    },
    /// A named IANA zone.
    Named(Tz),
}

impl SessionTimeZone {
    /// UTC, the zone stored `TIMESTAMP` values are held in.
    #[must_use]
    pub fn utc() -> Self {
        Self::Fixed {
            name: "UTC".to_owned(),
            offset_secs: 0,
        }
    }

    /// Whether this zone is UTC, which is what lets the storage codecs skip
    /// the conversion exactly where Go's `loc != time.UTC` guard does.
    #[must_use]
    pub fn is_utc(&self) -> bool {
        match self {
            Self::Fixed { offset_secs, .. } => *offset_secs == 0,
            Self::Named(zone) => *zone == Tz::UTC,
        }
    }
}

impl Default for SessionTimeZone {
    fn default() -> Self {
        Self::utc()
    }
}

/// The resolved offset of a [`SessionTimeZone`] at one instant.
///
/// `chrono` requires a zone's offset to be its own type; this carries the
/// zone back so `Offset::from_offset` can reconstruct it, which is what lets
/// a `DateTime<SessionTimeZone>` be re-projected into another zone.
#[derive(Clone, Debug, PartialEq, Eq, Copy)]
pub struct SessionTimeZoneOffset {
    fixed: FixedOffset,
}

impl Offset for SessionTimeZoneOffset {
    fn fix(&self) -> FixedOffset {
        self.fixed
    }
}

impl std::fmt::Display for SessionTimeZoneOffset {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.fixed.fmt(formatter)
    }
}

/// Lifts a `LocalResult` over one zone's offset into this zone's offset,
/// keeping the ambiguity/gap verdict intact -- a local time that does not
/// exist in a named zone must stay `LocalResult::None` here, because that is
/// the DST-gap error Go reports for a `TIMESTAMP` written during a spring
/// forward.
fn lift<O: Offset>(result: LocalResult<O>) -> LocalResult<SessionTimeZoneOffset> {
    match result {
        LocalResult::None => LocalResult::None,
        LocalResult::Single(offset) => LocalResult::Single(SessionTimeZoneOffset {
            fixed: offset.fix(),
        }),
        LocalResult::Ambiguous(earliest, latest) => LocalResult::Ambiguous(
            SessionTimeZoneOffset {
                fixed: earliest.fix(),
            },
            SessionTimeZoneOffset {
                fixed: latest.fix(),
            },
        ),
    }
}

impl TimeZone for SessionTimeZone {
    type Offset = SessionTimeZoneOffset;

    fn from_offset(offset: &Self::Offset) -> Self {
        Self::Fixed {
            name: offset.fixed.to_string(),
            offset_secs: offset.fixed.local_minus_utc(),
        }
    }

    fn offset_from_local_date(&self, local: &NaiveDate) -> LocalResult<Self::Offset> {
        match self {
            Self::Fixed { offset_secs, .. } => {
                lift(fixed(*offset_secs).offset_from_local_date(local))
            }
            Self::Named(zone) => lift(zone.offset_from_local_date(local)),
        }
    }

    fn offset_from_local_datetime(&self, local: &NaiveDateTime) -> LocalResult<Self::Offset> {
        match self {
            Self::Fixed { offset_secs, .. } => {
                lift(fixed(*offset_secs).offset_from_local_datetime(local))
            }
            Self::Named(zone) => lift(zone.offset_from_local_datetime(local)),
        }
    }

    fn offset_from_utc_date(&self, utc: &NaiveDate) -> Self::Offset {
        match self {
            Self::Fixed { offset_secs, .. } => SessionTimeZoneOffset {
                fixed: fixed(*offset_secs).offset_from_utc_date(utc).fix(),
            },
            Self::Named(zone) => SessionTimeZoneOffset {
                fixed: zone.offset_from_utc_date(utc).fix(),
            },
        }
    }

    fn offset_from_utc_datetime(&self, utc: &NaiveDateTime) -> Self::Offset {
        match self {
            Self::Fixed { offset_secs, .. } => SessionTimeZoneOffset {
                fixed: fixed(*offset_secs).offset_from_utc_datetime(utc).fix(),
            },
            Self::Named(zone) => SessionTimeZoneOffset {
                fixed: zone.offset_from_utc_datetime(utc).fix(),
            },
        }
    }
}

/// The `FixedOffset` for `offset_secs`, clamped into `chrono`'s representable
/// range. MySQL's own `time_zone` grammar caps the offset far inside it
/// (`-14:00`..`+14:00`), so the clamp is unreachable from SQL.
fn fixed(offset_secs: i32) -> FixedOffset {
    FixedOffset::east_opt(offset_secs.clamp(-86_399, 86_399)).expect("clamped into range")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn naive(text: &str) -> NaiveDateTime {
        NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S").expect("parsable")
    }

    #[test]
    fn fixed_zone_projects_like_its_offset() {
        let zone = SessionTimeZone::Fixed {
            name: "+08:00".to_owned(),
            offset_secs: 8 * 3600,
        };
        let utc = naive("2020-01-03 07:16:59");
        let local = chrono::Utc
            .from_utc_datetime(&utc)
            .with_timezone(&zone)
            .naive_local();
        assert_eq!(local, naive("2020-01-03 15:16:59"));
    }

    /// The whole reason the named arm exists: a fixed offset would answer
    /// the same hour on both sides of a DST boundary.
    #[test]
    fn named_zone_follows_daylight_saving() {
        let zone = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        let before = chrono::Utc
            .from_utc_datetime(&naive("2021-03-14 09:30:00"))
            .with_timezone(&zone)
            .naive_local();
        let after = chrono::Utc
            .from_utc_datetime(&naive("2021-03-14 11:30:00"))
            .with_timezone(&zone)
            .naive_local();
        assert_eq!(before, naive("2021-03-14 01:30:00"));
        assert_eq!(after, naive("2021-03-14 04:30:00"));
    }

    /// A local time inside the spring-forward gap does not exist, and the
    /// verdict has to survive the lift or the DST diagnostic is lost.
    #[test]
    fn nonexistent_local_time_stays_none() {
        let zone = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        assert!(matches!(
            zone.offset_from_local_datetime(&naive("2021-03-14 02:30:00")),
            LocalResult::None
        ));
    }

    /// A local time the fall-back repeats is ambiguous, and `chrono`'s
    /// earliest-wins resolution is what Go's `time.Date` picks too.
    #[test]
    fn repeated_local_time_stays_ambiguous() {
        let zone = SessionTimeZone::Named(chrono_tz::America::Los_Angeles);
        assert!(matches!(
            zone.offset_from_local_datetime(&naive("2021-11-07 01:30:00")),
            LocalResult::Ambiguous(_, _)
        ));
    }

    #[test]
    fn utc_is_recognised_in_both_spellings() {
        assert!(SessionTimeZone::utc().is_utc());
        assert!(SessionTimeZone::Named(chrono_tz::UTC).is_utc());
        assert!(!SessionTimeZone::Named(chrono_tz::America::Los_Angeles).is_utc());
        assert!(!SessionTimeZone::Fixed {
            name: "+08:00".to_owned(),
            offset_secs: 8 * 3600,
        }
        .is_utc());
    }

    #[test]
    fn dates_project_through_both_arms() {
        let day = NaiveDate::from_ymd_opt(2021, 7, 1).expect("valid date");
        for zone in [
            SessionTimeZone::Named(chrono_tz::America::Los_Angeles),
            SessionTimeZone::Fixed {
                name: "-07:00".to_owned(),
                offset_secs: -7 * 3600,
            },
        ] {
            assert_eq!(
                zone.offset_from_utc_date(&day).fix().local_minus_utc(),
                -7 * 3600
            );
            assert!(matches!(
                zone.offset_from_local_date(&day),
                LocalResult::Single(_) | LocalResult::Ambiguous(_, _)
            ));
        }
    }
}
