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

//! What a `TIMESTAMP` column is FOR: a value written in one session's
//! `time_zone` and read back in another's is the same INSTANT, rendered in
//! the reader's zone. `DATETIME` and `DATE` are the opposite promise -- the
//! wall-clock text is stored and returned unchanged -- and the two promises
//! are pinned side by side in every test here, from the same two sessions,
//! because a fix that converts BOTH types is wrong in the other direction
//! and a `TIMESTAMP`-only test cannot see it.
//!
//! The decision is Go's and it is made ONCE, on the TYPE:
//!
//! ```go
//! // encoding, on the way into the row bytes
//! if t.Type() == mysql.TypeTimestamp && loc != nil && loc != time.UTC {
//!     err := t.ConvertTimeZone(loc, time.UTC)
//! }
//! // decoding, on the way out
//! if ft.GetType() == mysql.TypeTimestamp && !t.IsZero() {
//!     err = t.ConvertTimeZone(time.UTC, loc)
//! }
//! ```
//!
//! # Which code that is, exactly
//!
//! There are THREE places it lives, and naming only the first is how a
//! mutation probe comes back green against a real bug:
//!
//! * the v2 row format, `tidb_codec::rowcodec`'s `encode_column_value` and
//!   `decode_column_value` (Go `pkg/util/rowcodec/encoder.go` and
//!   `decoder.go`), which is what a byte-backed table actually stores;
//! * index keys, `tidb_codec::package::encode_mysql_time` under
//!   `encode_key_in_timezone` (Go `codec.EncodeKey`, which
//!   `tablecodec.GenIndexKey(enc, loc, ...)` calls), so the entry one
//!   session files is the entry another seeks -- without it an index read
//!   silently returns nothing where a full scan returns the row;
//! * the v1 row format, `tidb_tablecodec::flatten_datum`/`unflatten_datum`
//!   (Go `pkg/tablecodec/tablecodec.go`'s `flatten`/`unflatten`), which this
//!   node does not write but keeps for parity.
//!
//! Each of the first two is pinned independently: neutering the row codec
//! fails nine of the tests below, neutering the key codec fails exactly the
//! two index ones, and neither probe disturbs the DATETIME assertions.
//!
//! # The captures
//!
//! Every expectation below came from real TiDB through
//! `rust/difftests/gorun` before any of this was written, driving one session
//! that changes `time_zone` between the write and the read. Times were chosen
//! so a wrong answer is visible as a wrong DAY, not merely a wrong hour:
//!
//! ```text
//! SET time_zone='+08:00'; INSERT ... VALUES ('2020-01-03 15:16:59', <same>, '2020-01-03')
//!                                            ts                     dt      d
//!   read at +08:00 -> 2020-01-03 15:16:59 | 2020-01-03 15:16:59 | 2020-01-03
//!   read at +00:00 -> 2020-01-03 07:16:59 | 2020-01-03 15:16:59 | 2020-01-03
//!   read at -08:00 -> 2020-01-02 23:16:59 | 2020-01-03 15:16:59 | 2020-01-03
//!
//! SET time_zone='+08:00'; INSERT ... VALUES ('2020-01-03 02:00:00', <same>, ...)
//!   read at +00:00 -> 2020-01-02 18:00:00 | 2020-01-03 02:00:00   (the day moves BACK)
//!   read at -08:00 -> 2020-01-02 10:00:00 | 2020-01-03 02:00:00
//!
//! SET time_zone='+00:00'; INSERT ... VALUES ('2020-01-03 23:30:00', <same>)
//!   read at +08:00 -> 2020-01-04 07:30:00 | 2020-01-03 23:30:00   (the day moves FORWARD)
//! ```
//!
//! DST, which is where a fixed-offset shortcut breaks and a `+08:00`-only
//! test passes anyway. `America/Los_Angeles` is `-08:00` before the
//! 2021-03-14 spring forward and `-07:00` after it:
//!
//! ```text
//! written at +00:00, read at America/Los_Angeles
//!   2021-03-14 09:30:00 -> 2021-03-14 01:30:00   (PST, -8)
//!   2021-03-14 11:30:00 -> 2021-03-14 04:30:00   (PDT, -7)
//!   2021-11-07 08:30:00 -> 2021-11-07 01:30:00   \ the fall-back repeats
//!   2021-11-07 09:30:00 -> 2021-11-07 01:30:00   / this local hour
//!
//! written at America/Los_Angeles, read at +00:00
//!   2021-03-14 01:30:00 -> 2021-03-14 09:30:00
//!   2021-03-14 03:30:00 -> 2021-03-14 10:30:00
//!   2021-11-07 01:30:00 -> 2021-11-07 08:30:00   (the earlier of the two)
//! ```
//!
//! The epoch boundaries, whose stored form is the same in every zone and
//! whose RENDERED form is not -- `1970-01-01 00:00:01` reads back as
//! `1969-12-31 16:00:01` at `-08:00`, which is outside the range the write
//! would have accepted and is still what TiDB returns:
//!
//! ```text
//! SET time_zone='+00:00'; INSERT ... VALUES ('1970-01-01 00:00:01'), ('2038-01-19 03:14:07')
//!   read at +00:00 -> 1970-01-01 00:00:01 | 2038-01-19 03:14:07
//!   read at +08:00 -> 1970-01-01 08:00:01 | 2038-01-19 11:14:07
//!   read at -08:00 -> 1969-12-31 16:00:01 | 2038-01-18 19:14:07
//! ```
//!
//! NULL is NULL in every zone. The all-zero value never reaches storage at
//! all under the default SQL mode (`tests_zero_date` owns that matrix), so
//! there is nothing for the conversion to interact with -- and every codec
//! above guards the zero value anyway, which the round trip of a
//! zero-permitting mode pins here.
//!
//! `CURRENT_TIMESTAMP` as a `DEFAULT` is EVALUATED rather than stored
//! literally, so its stored instant is zone-free; the visible fact is that
//! the `TIMESTAMP` it filled and the `DATETIME` it filled at the same moment
//! agree at `+00:00` and differ by the offset anywhere else:
//!
//! ```text
//! SET time_zone='+00:00'; INSERT INTO ct (id) VALUES (1);
//!   SELECT TIMESTAMPDIFF(HOUR, ts, dt) at +00:00 -> 0
//!   SELECT TIMESTAMPDIFF(HOUR, ts, dt) at +08:00 -> -8
//! ```
//!
//! # Not covered here
//!
//! TIMESTAMP's epoch-RANGE check is zone-dependent too -- `SET
//! time_zone='+08:00'; CREATE TABLE t (ts TIMESTAMP DEFAULT '1970-01-01
//! 00:30:00')` is 1067 in TiDB -- but it is a CONVERSION-seam behaviour
//! (`Datum::convert_to_time_target`), not a storage one, so it has its own
//! file: `tests_timestamp_range`. The two seams are independent: neutering
//! the conversion's zone fails all four tests there and none here.

use super::Session;
use crate::tests_support::row_text;

/// A session with a database selected, ready for the fixtures below.
fn session() -> Session {
    let mut session = Session::new();
    session.run("CREATE DATABASE tz").expect("database");
    session.run("USE tz").expect("use");
    session
}

fn set_zone(session: &mut Session, zone: &str) {
    session
        .run(&format!("SET time_zone = '{zone}'"))
        .unwrap_or_else(|error| panic!("SET time_zone = '{zone}' failed: {error:?}"));
}

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// Reads `sql` from `zone`, so each assertion names the reading session.
fn read_at(session: &mut Session, zone: &str, sql: &str) -> Vec<Vec<String>> {
    set_zone(session, zone);
    rows(session, sql)
}

/// Writing at `+08:00` and reading at three zones: the `TIMESTAMP` follows
/// the reader, the `DATETIME` and the `DATE` do not.
///
/// Row 2's `02:00:00` is the control that makes an off-by-one DAY visible:
/// at `+00:00` it is the PREVIOUS day.
#[test]
fn timestamp_follows_the_reading_session_and_datetime_does_not() {
    let mut session = session();
    set_zone(&mut session, "+08:00");
    session
        .run("CREATE TABLE tsd (id INT PRIMARY KEY, ts TIMESTAMP, dt DATETIME, d DATE)")
        .expect("create");
    session
        .run(
            "INSERT INTO tsd VALUES \
             (1, '2020-01-03 15:16:59', '2020-01-03 15:16:59', '2020-01-03'), \
             (2, '2020-01-03 02:00:00', '2020-01-03 02:00:00', '2020-01-03')",
        )
        .expect("insert");

    let select = "SELECT id, ts, dt, d FROM tsd ORDER BY id";
    for (zone, first_ts, second_ts) in [
        ("+08:00", "2020-01-03 15:16:59", "2020-01-03 02:00:00"),
        ("+00:00", "2020-01-03 07:16:59", "2020-01-02 18:00:00"),
        ("-08:00", "2020-01-02 23:16:59", "2020-01-02 10:00:00"),
    ] {
        assert_eq!(
            read_at(&mut session, zone, select),
            [
                vec![
                    "1".to_owned(),
                    first_ts.to_owned(),
                    // The contrast, in the same row as the TIMESTAMP that
                    // moved: DATETIME and DATE are the written text in
                    // every zone.
                    "2020-01-03 15:16:59".to_owned(),
                    "2020-01-03".to_owned(),
                ],
                vec![
                    "2".to_owned(),
                    second_ts.to_owned(),
                    "2020-01-03 02:00:00".to_owned(),
                    "2020-01-03".to_owned(),
                ],
            ],
            "read from {zone}"
        );
    }
}

/// The other direction: written at `+00:00`, read at `+08:00`, with a time
/// late enough that the day moves FORWARD.
#[test]
fn timestamp_written_in_utc_reads_forward_in_an_eastern_session() {
    let mut session = session();
    set_zone(&mut session, "+00:00");
    session
        .run("CREATE TABLE tsu (id INT PRIMARY KEY, ts TIMESTAMP, dt DATETIME)")
        .expect("create");
    session
        .run("INSERT INTO tsu VALUES (1, '2020-01-03 23:30:00', '2020-01-03 23:30:00')")
        .expect("insert");

    let select = "SELECT id, ts, dt FROM tsu ORDER BY id";
    assert_eq!(
        read_at(&mut session, "+00:00", select),
        [[
            "1".to_owned(),
            "2020-01-03 23:30:00".to_owned(),
            "2020-01-03 23:30:00".to_owned()
        ]]
    );
    assert_eq!(
        read_at(&mut session, "+08:00", select),
        [[
            "1".to_owned(),
            "2020-01-04 07:30:00".to_owned(),
            // Same instant, next day -- but only for the TIMESTAMP.
            "2020-01-03 23:30:00".to_owned()
        ]]
    );
}

/// A named zone with daylight saving applies the offset IN FORCE AT THAT
/// INSTANT, which a fixed offset cannot do.
///
/// The two March rows are 2 hours apart in UTC and 3 hours apart in local
/// time, because the spring forward happened between them; a `-08:00`
/// shortcut would answer `03:30:00` for the second. The two November rows
/// are 1 hour apart in UTC and render as the SAME local time, because the
/// fall-back repeats that hour.
#[test]
fn a_named_zone_reads_back_across_a_daylight_saving_boundary() {
    let mut session = session();
    set_zone(&mut session, "+00:00");
    session
        .run("CREATE TABLE dst (id INT PRIMARY KEY, ts TIMESTAMP, dt DATETIME)")
        .expect("create");
    session
        .run(
            "INSERT INTO dst VALUES \
             (1, '2021-03-14 09:30:00', '2021-03-14 09:30:00'), \
             (2, '2021-03-14 11:30:00', '2021-03-14 11:30:00'), \
             (3, '2021-11-07 08:30:00', '2021-11-07 08:30:00'), \
             (4, '2021-11-07 09:30:00', '2021-11-07 09:30:00')",
        )
        .expect("insert");

    let select = "SELECT id, ts, dt FROM dst ORDER BY id";
    assert_eq!(
        read_at(&mut session, "America/Los_Angeles", select),
        [
            ["1", "2021-03-14 01:30:00", "2021-03-14 09:30:00"],
            ["2", "2021-03-14 04:30:00", "2021-03-14 11:30:00"],
            ["3", "2021-11-07 01:30:00", "2021-11-07 08:30:00"],
            ["4", "2021-11-07 01:30:00", "2021-11-07 09:30:00"],
        ]
    );
    // The DATETIME column above never moved. Back at UTC every TIMESTAMP is
    // the text it was written as, which proves the read converted rather
    // than the write having damaged the stored value.
    assert_eq!(
        read_at(&mut session, "+00:00", select),
        [
            ["1", "2021-03-14 09:30:00", "2021-03-14 09:30:00"],
            ["2", "2021-03-14 11:30:00", "2021-03-14 11:30:00"],
            ["3", "2021-11-07 08:30:00", "2021-11-07 08:30:00"],
            ["4", "2021-11-07 09:30:00", "2021-11-07 09:30:00"],
        ]
    );
}

/// The WRITE side of the same boundary: a local time in a DST zone is
/// converted with that instant's own offset, and the repeated fall-back hour
/// resolves to the earlier of its two instants.
#[test]
fn a_named_zone_writes_across_a_daylight_saving_boundary() {
    let mut session = session();
    set_zone(&mut session, "America/Los_Angeles");
    session
        .run("CREATE TABLE dstw (id INT PRIMARY KEY, ts TIMESTAMP, dt DATETIME)")
        .expect("create");
    session
        .run(
            "INSERT INTO dstw VALUES \
             (1, '2021-03-14 01:30:00', '2021-03-14 01:30:00'), \
             (2, '2021-03-14 03:30:00', '2021-03-14 03:30:00'), \
             (3, '2021-11-07 01:30:00', '2021-11-07 01:30:00')",
        )
        .expect("insert");

    let select = "SELECT id, ts, dt FROM dstw ORDER BY id";
    assert_eq!(
        read_at(&mut session, "America/Los_Angeles", select),
        [
            ["1", "2021-03-14 01:30:00", "2021-03-14 01:30:00"],
            ["2", "2021-03-14 03:30:00", "2021-03-14 03:30:00"],
            ["3", "2021-11-07 01:30:00", "2021-11-07 01:30:00"],
        ]
    );
    assert_eq!(
        read_at(&mut session, "+00:00", select),
        [
            // -08:00 before the spring forward, -07:00 after it.
            ["1", "2021-03-14 09:30:00", "2021-03-14 01:30:00"],
            ["2", "2021-03-14 10:30:00", "2021-03-14 03:30:00"],
            ["3", "2021-11-07 08:30:00", "2021-11-07 01:30:00"],
        ]
    );
}

/// The epoch boundaries survive the round trip, and their RENDERED form in a
/// distant zone falls outside the range a write would have accepted -- which
/// is what TiDB returns and is not an error on the read path.
#[test]
fn the_epoch_boundaries_render_in_the_reading_session() {
    let mut session = session();
    set_zone(&mut session, "+00:00");
    session
        .run("CREATE TABLE ep (id INT PRIMARY KEY, ts TIMESTAMP)")
        .expect("create");
    session
        .run("INSERT INTO ep VALUES (1, '1970-01-01 00:00:01'), (2, '2038-01-19 03:14:07')")
        .expect("insert");

    let select = "SELECT id, ts FROM ep ORDER BY id";
    for (zone, low, high) in [
        ("+00:00", "1970-01-01 00:00:01", "2038-01-19 03:14:07"),
        ("+08:00", "1970-01-01 08:00:01", "2038-01-19 11:14:07"),
        ("-08:00", "1969-12-31 16:00:01", "2038-01-18 19:14:07"),
    ] {
        assert_eq!(
            read_at(&mut session, zone, select),
            [["1", low], ["2", high]],
            "read from {zone}"
        );
    }
}

/// NULL is NULL in every zone, and a zero `TIMESTAMP` -- the one value
/// every codec guards as the zero -- is the zero value in every
/// zone rather than the zero value shifted by an offset.
#[test]
fn null_and_the_zero_timestamp_do_not_move() {
    let mut session = session();
    session.run("SET sql_mode = ''").expect("sql_mode");
    set_zone(&mut session, "+08:00");
    session
        .run("CREATE TABLE zt (id INT PRIMARY KEY, ts TIMESTAMP NULL, dt DATETIME NULL)")
        .expect("create");
    session
        .run("INSERT INTO zt VALUES (1, NULL, NULL), (2, '0000-00-00 00:00:00', '0000-00-00 00:00:00')")
        .expect("insert");

    let select = "SELECT id, ts, dt FROM zt ORDER BY id";
    for zone in ["+08:00", "+00:00", "-08:00"] {
        assert_eq!(
            read_at(&mut session, zone, select),
            [
                ["1", "NULL", "NULL"],
                ["2", "0000-00-00 00:00:00", "0000-00-00 00:00:00"],
            ],
            "read from {zone}"
        );
    }
}

/// A `DEFAULT CURRENT_TIMESTAMP` is evaluated, not stored as text, so the
/// `TIMESTAMP` and the `DATETIME` it filled at the same moment agree in the
/// writing session's zone and differ by the offset in any other.
#[test]
fn a_current_timestamp_default_separates_from_its_datetime_twin_by_the_offset() {
    let mut session = session();
    set_zone(&mut session, "+00:00");
    session
        .run(
            "CREATE TABLE ct (id INT PRIMARY KEY, \
             ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, \
             dt DATETIME DEFAULT CURRENT_TIMESTAMP)",
        )
        .expect("create");
    session
        .run("INSERT INTO ct (id) VALUES (1)")
        .expect("insert");

    let select = "SELECT TIMESTAMPDIFF(HOUR, ts, dt) FROM ct";
    assert_eq!(read_at(&mut session, "+00:00", select), [["0"]]);
    assert_eq!(read_at(&mut session, "+08:00", select), [["-8"]]);
}

/// An INDEX over a `TIMESTAMP` column is filed under the same stored value
/// the row is, so a lookup from a different zone finds the row: Go threads
/// the statement's `loc` into `GenIndexKey` for exactly this reason.
///
/// Without it the entry written at `+08:00` and the entry sought at `+00:00`
/// are different keys, and the index read silently returns nothing while the
/// full scan returns the row -- a divergence between two access paths over
/// the same table.
#[test]
fn an_index_over_a_timestamp_is_found_from_another_zone() {
    let mut session = session();
    set_zone(&mut session, "+08:00");
    session
        .run("CREATE TABLE tsi (id INT PRIMARY KEY, ts TIMESTAMP, KEY k (ts))")
        .expect("create");
    session
        .run("INSERT INTO tsi VALUES (1, '2020-01-03 15:16:59')")
        .expect("insert");

    set_zone(&mut session, "+00:00");
    // The same instant, named in the reading session's own zone.
    assert_eq!(
        rows(
            &mut session,
            "SELECT id FROM tsi WHERE ts = '2020-01-03 07:16:59'"
        ),
        [["1"]]
    );
    // And the text the WRITER used is now a different instant, so it matches
    // nothing -- the read is converting, not comparing raw text.
    assert!(rows(
        &mut session,
        "SELECT id FROM tsi WHERE ts = '2020-01-03 15:16:59'"
    )
    .is_empty());
    session
        .run("ADMIN CHECK TABLE tsi")
        .expect("the stored entries re-encode from the rows they name");
}

/// An UPDATE and a DELETE reach the row they stored: both rebuild the index
/// entry to remove it, so a zone that reached the write and not the rewrite
/// would leave an orphaned entry behind.
#[test]
fn update_and_delete_maintain_a_timestamp_index_in_another_zone() {
    let mut session = session();
    set_zone(&mut session, "+08:00");
    session
        .run("CREATE TABLE tsm (id INT PRIMARY KEY, ts TIMESTAMP, KEY k (ts))")
        .expect("create");
    session
        .run("INSERT INTO tsm VALUES (1, '2020-01-03 15:16:59'), (2, '2020-01-04 15:16:59')")
        .expect("insert");

    set_zone(&mut session, "+00:00");
    session
        .run("UPDATE tsm SET ts = '2021-06-01 00:30:00' WHERE id = 1")
        .expect("update");
    session.run("DELETE FROM tsm WHERE id = 2").expect("delete");

    assert_eq!(
        rows(&mut session, "SELECT id, ts FROM tsm ORDER BY id"),
        [["1", "2021-06-01 00:30:00"]]
    );
    assert_eq!(
        rows(
            &mut session,
            "SELECT id FROM tsm WHERE ts = '2021-06-01 00:30:00'"
        ),
        [["1"]]
    );
    session
        .run("ADMIN CHECK TABLE tsm")
        .expect("no entry of a deleted or rewritten row is left behind");
    // The surviving row is still the same instant seen from the zone that
    // wrote the table.
    assert_eq!(
        read_at(&mut session, "+08:00", "SELECT ts FROM tsm"),
        [["2021-06-01 08:30:00"]]
    );
}

/// A wall-clock time the fall-back REPEATS has two real instants, and which
/// one TiDB picks is a property of `time.Date`'s arithmetic rather than a
/// rule like "always the earlier". Two zones answer opposite ways, which is
/// why both are pinned:
///
/// ```text
/// America/Los_Angeles  '2021-11-07 01:00:00' -> 2021-11-07 08:00:00 UTC  (earlier, PDT)
///                      '2021-11-07 01:30:00' -> 2021-11-07 08:30:00 UTC
///                      '2021-11-07 01:59:59' -> 2021-11-07 08:59:59 UTC
/// Europe/London        '2021-10-31 01:30:00' -> 2021-10-31 01:30:00 UTC  (later, GMT)
/// ```
#[test]
fn a_repeated_local_time_resolves_the_way_go_time_date_does() {
    let mut session = session();
    set_zone(&mut session, "America/Los_Angeles");
    session
        .run("CREATE TABLE amb (id INT PRIMARY KEY, ts TIMESTAMP)")
        .expect("create");
    session
        .run(
            "INSERT INTO amb VALUES \
             (1, '2021-11-07 01:00:00'), \
             (2, '2021-11-07 01:30:00'), \
             (3, '2021-11-07 01:59:59')",
        )
        .expect("insert");
    assert_eq!(
        read_at(&mut session, "+00:00", "SELECT id, ts FROM amb ORDER BY id"),
        [
            ["1", "2021-11-07 08:00:00"],
            ["2", "2021-11-07 08:30:00"],
            ["3", "2021-11-07 08:59:59"],
        ]
    );

    set_zone(&mut session, "Europe/London");
    session
        .run("CREATE TABLE amb2 (id INT PRIMARY KEY, ts TIMESTAMP)")
        .expect("create");
    session
        .run("INSERT INTO amb2 VALUES (1, '2021-10-31 01:30:00')")
        .expect("insert");
    assert_eq!(
        read_at(
            &mut session,
            "+00:00",
            "SELECT id, ts FROM amb2 ORDER BY id"
        ),
        [["1", "2021-10-31 01:30:00"]]
    );
}

/// The same resolution seen without any storage at all, so a future change to
/// the codecs cannot make this pass for the wrong reason.
///
/// Captured at `America/Los_Angeles`: the repeated `01:30` takes the earlier
/// instant, and its unambiguous neighbours bracket it.
#[test]
fn unix_timestamp_agrees_with_the_stored_resolution() {
    let mut session = session();
    set_zone(&mut session, "America/Los_Angeles");
    assert_eq!(
        rows(
            &mut session,
            "SELECT UNIX_TIMESTAMP('2021-11-07 00:30:00'), \
                    UNIX_TIMESTAMP('2021-11-07 01:30:00'), \
                    UNIX_TIMESTAMP('2021-11-07 02:30:00')"
        ),
        [["1636270200", "1636273800", "1636281000"]]
    );
}
