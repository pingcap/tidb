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

//! Which `TIMESTAMP` literals a column ADMITS moves with the session's
//! `time_zone`, on the DDL `DEFAULT` path and on the write path alike.
//!
//! `MinTimestamp`/`MaxTimestamp` are `1970-01-01 00:00:01` and
//! `2038-01-19 03:14:07.999999` **in UTC**, and Go's `checkTimestampType`
//! converts the written wall-clock value OUT of `ctx.Location()` into UTC
//! before comparing:
//!
//! ```go
//! func checkTimestampType(t CoreTime, tz *gotime.Location) error {
//!     if tz != BoundTimezone {                       // BoundTimezone = time.UTC
//!         convertTime := NewTime(t, mysql.TypeTimestamp, DefaultFsp)
//!         err := convertTime.ConvertTimeZone(tz, BoundTimezone)
//!         ...
//!         checkTime = convertTime.coreTime
//!     }
//!     if compareTime(checkTime, MaxTimestamp.coreTime) > 0 ||
//!         compareTime(checkTime, MinTimestamp.coreTime) < 0 {
//!         return ErrWrongValue...
//!     }
//! ```
//!
//! `types.ParseTime(ctx, ...)` reaches it through `Time.Check(ctx)`, and the
//! Rust conversion previously handed `Datum::convert_to_time_target` a
//! hardcoded `Utc`, so the window never moved and every literal below was
//! ACCEPTED in every zone.
//!
//! # The captures
//!
//! Real TiDB through `rust/difftests/gorun`, one session per zone, error
//! numbers taken from a `terror.ToSQLError` probe over the same statements
//! (`gorun` prints a bare `ERR`):
//!
//! ```text
//! DDL   ts timestamp default '1970-01-01 00:30:00'     +00:00 OK   -08:00 OK    +08:00 1067
//! DDL   ts timestamp default '2038-01-19 03:14:07'     +00:00 OK   +08:00 OK    -08:00 1067
//! DDL   ts timestamp default '2000-01-01 12:00:00'     OK in every zone (the control)
//! DDL   ts timestamp default '1969-12-31 16:00:01'     -08:00 OK   (== the epoch, rendered there)
//! DDL   ts timestamp default '1970-01-01 08:00:01'     +08:00 OK   (== the epoch, rendered there)
//!
//! WRITE SET time_zone='+08:00'; INSERT '1970-01-01 00:30:00'                    1292
//! WRITE SET time_zone='-08:00'; INSERT '2038-01-19 03:14:07'                    1292
//! WRITE SET time_zone='-08:00'; INSERT '1969-12-31 16:00:01'                    OK
//! ```
//!
//! # The DST zone earns its place
//!
//! A fixed offset cannot see either of these, and both are real:
//!
//! ```text
//! DDL   America/Los_Angeles  default '2038-01-19 03:14:07'   1067   (-08:00 in January)
//! DDL   America/Los_Angeles  default '2038-01-18 19:14:07'   OK     (the max, rendered there)
//! DDL   America/Los_Angeles  default '2024-03-10 02:30:00'   1067   (the spring-forward GAP)
//! WRITE America/Los_Angeles  INSERT  '2024-03-10 02:30:00'   1292
//! WRITE America/Los_Angeles  INSERT  '2038-01-18 19:14:08'   1292   (one second past the max)
//! ```
//!
//! `2024-03-10 02:30:00` is a wall-clock reading that DOES NOT EXIST in Los
//! Angeles -- the clock jumps 02:00 to 03:00 -- and it is comfortably inside
//! the epoch window, so only the zone's own rules can refuse it. Go reaches
//! it through `ConvertTimeZone` failing and `adjustTimestampErrForDST`.
//!
//! # The asymmetry, and why the READ path must not grow this check
//!
//! The window is a window on the INSTANT, not on the rendered text, so a
//! stored value can render OUTSIDE the range the writing zone would admit:
//!
//! ```text
//! SET time_zone='+00:00'; INSERT INTO r VALUES ('1970-01-01 00:00:01')  -- the epoch itself
//!   read at +00:00              -> 1970-01-01 00:00:01
//!   read at -08:00              -> 1969-12-31 16:00:01
//!   read at America/Los_Angeles -> 1969-12-31 16:00:01
//! ```
//!
//! `1969-12-31 16:00:01` is a 1969 date. Applying the write path's bound on
//! READ would refuse a row TiDB serves -- a wrong-REJECT strictly worse than
//! the wrong-ACCEPT this file closes. [`the_epoch_reads_back_as_a_1969_date_at_minus_eight`]
//! pins that, so a later tightening cannot quietly take it away.

use super::Session;
use crate::tests_support::row_text;

fn session() -> Session {
    let mut session = Session::new();
    session.run("CREATE DATABASE tsr").expect("database");
    session.run("USE tsr").expect("use");
    session
}

fn set_zone(session: &mut Session, zone: &str) {
    session
        .run(&format!("SET time_zone = '{zone}'"))
        .unwrap_or_else(|error| panic!("SET time_zone = '{zone}' failed: {error:?}"));
}

/// `None` when the statement succeeded, else its MySQL error number.
fn code(session: &mut Session, sql: &str) -> Option<u16> {
    match session.run(sql) {
        Ok(_) => None,
        Err(error) => Some(error.to_mysql_error().code),
    }
}

/// The DDL `DEFAULT` path, both ends of the window, in four zones.
#[test]
fn a_timestamp_default_is_range_checked_in_the_session_zone() {
    let mut session = session();
    let mut table = 0;
    let mut check = |session: &mut Session, zone: &str, literal: &str, expected: Option<u16>| {
        set_zone(session, zone);
        table += 1;
        let sql = format!("CREATE TABLE d{table} (ts TIMESTAMP DEFAULT '{literal}')");
        assert_eq!(code(session, &sql), expected, "{zone}: {literal}");
    };

    // Just above the epoch in UTC: refused wherever the zone pushes it below.
    check(&mut session, "+00:00", "1970-01-01 00:30:00", None);
    check(&mut session, "-08:00", "1970-01-01 00:30:00", None);
    check(&mut session, "+08:00", "1970-01-01 00:30:00", Some(1067));

    // The maximum in UTC: refused wherever the zone pushes it above.
    check(&mut session, "+00:00", "2038-01-19 03:14:07", None);
    check(&mut session, "+08:00", "2038-01-19 03:14:07", None);
    check(&mut session, "-08:00", "2038-01-19 03:14:07", Some(1067));

    // The bounds THEMSELVES, rendered in each zone, stay admissible -- the
    // control that separates a zone-aware window from a blanket refusal.
    check(&mut session, "-08:00", "1969-12-31 16:00:01", None);
    check(&mut session, "+08:00", "1970-01-01 08:00:01", None);

    // A value in the middle is accepted in every zone.
    for zone in ["+00:00", "-08:00", "+08:00", "America/Los_Angeles"] {
        check(&mut session, zone, "2000-01-01 12:00:00", None);
    }
}

/// The same DDL path under a zone with DST rules, which a fixed offset
/// cannot stand in for.
#[test]
fn a_timestamp_default_is_range_checked_under_a_dst_zone() {
    let mut session = session();
    let mut table = 0;
    let mut check = |session: &mut Session, literal: &str, expected: Option<u16>| {
        set_zone(session, "America/Los_Angeles");
        table += 1;
        let sql = format!("CREATE TABLE g{table} (ts TIMESTAMP DEFAULT '{literal}')");
        assert_eq!(code(session, &sql), expected, "Los Angeles: {literal}");
    };

    // January is PST (-08:00), so the UTC maximum overflows and the maximum
    // AS RENDERED THERE does not.
    check(&mut session, "2038-01-19 03:14:07", Some(1067));
    check(&mut session, "2038-01-18 19:14:07", None);
    // January 1970 is PST too, so the epoch-adjacent value is admissible.
    check(&mut session, "1970-01-01 00:30:00", None);
    check(&mut session, "1969-12-31 16:00:01", None);
    // The spring-forward gap: a wall clock reading that never happens.
    check(&mut session, "2024-03-10 02:30:00", Some(1067));
    // The same day, one hour later, does happen.
    check(&mut session, "2024-03-10 03:30:00", None);
}

/// The write path: the same window, reported as 1292 per row.
#[test]
fn an_inserted_timestamp_is_range_checked_in_the_session_zone() {
    let mut session = session();
    session
        .run("CREATE TABLE t (ts TIMESTAMP)")
        .expect("create");

    for (zone, literal, expected) in [
        ("+00:00", "1970-01-01 00:30:00", None),
        ("-08:00", "1970-01-01 00:30:00", None),
        ("+08:00", "1970-01-01 00:30:00", Some(1292)),
        ("+00:00", "2038-01-19 03:14:07", None),
        ("+08:00", "2038-01-19 03:14:07", None),
        ("-08:00", "2038-01-19 03:14:07", Some(1292)),
        // The bounds as rendered in the writing zone are admissible.
        ("-08:00", "1969-12-31 16:00:01", None),
        ("+08:00", "1970-01-01 08:00:01", None),
        // The control: comfortably inside, accepted everywhere.
        ("+00:00", "2000-01-01 12:00:00", None),
        ("-08:00", "2000-01-01 12:00:00", None),
        ("+08:00", "2000-01-01 12:00:00", None),
        // DST: the gap, the maximum rendered locally, and one second past it.
        ("America/Los_Angeles", "2024-03-10 02:30:00", Some(1292)),
        ("America/Los_Angeles", "2038-01-18 19:14:07", None),
        ("America/Los_Angeles", "2038-01-18 19:14:08", Some(1292)),
        ("America/Los_Angeles", "1969-12-31 16:00:01", None),
    ] {
        set_zone(&mut session, zone);
        assert_eq!(
            code(&mut session, &format!("INSERT INTO t VALUES ('{literal}')")),
            expected,
            "{zone}: {literal}"
        );
    }
}

/// The read-side asymmetry, pinned so a later tightening cannot take it: the
/// EPOCH itself renders as a 1969 date west of Greenwich, which is outside
/// the window a write in that zone would admit, and TiDB still returns it.
#[test]
fn the_epoch_reads_back_as_a_1969_date_at_minus_eight() {
    let mut session = session();
    session
        .run("CREATE TABLE r (ts TIMESTAMP)")
        .expect("create");
    set_zone(&mut session, "+00:00");
    session
        .run("INSERT INTO r VALUES ('1970-01-01 00:00:01')")
        .expect("the epoch is admissible at +00:00");

    for (zone, rendered) in [
        ("+00:00", "1970-01-01 00:00:01"),
        ("-08:00", "1969-12-31 16:00:01"),
        ("America/Los_Angeles", "1969-12-31 16:00:01"),
    ] {
        set_zone(&mut session, zone);
        assert_eq!(
            row_text(session.run("SELECT ts FROM r")),
            [vec![rendered.to_owned()]],
            "read at {zone}"
        );
    }

    // And the same text, WRITTEN at -08:00, is admissible too: the bound is
    // on the instant, so the epoch is in range however it is spelled. This
    // is what makes the read above consistent rather than a leak.
    set_zone(&mut session, "-08:00");
    session
        .run("INSERT INTO r VALUES ('1969-12-31 16:00:01')")
        .expect("the epoch, spelled at -08:00, is admissible");
}
