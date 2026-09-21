//! Frame geometry: `ROWS` and `RANGE` bounds, the default
//! `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, interval bounds, and
//! the empty frame -- Go `pkg/executor/window.go`'s frame handling.

use crate::tests_support::*;
use crate::*;

#[test]
fn window_range_exhausted_cursor_skips_boundary_evaluation() {
    let mut session = Session::new();
    session.run("CREATE TABLE t(k BIGINT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1),(9223372036854775807)")
        .unwrap();
    for sql in [
        "CREATE TABLE start_signed(k BIGINT)",
        "INSERT INTO start_signed VALUES (9223372036854775805),(9223372036854775806)",
        "CREATE TABLE start_unsigned(k BIGINT UNSIGNED)",
        "INSERT INTO start_unsigned VALUES (18446744073709551613),(18446744073709551614)",
        "CREATE TABLE end_unsigned(k BIGINT UNSIGNED)",
        "INSERT INTO end_unsigned VALUES (18446744073709551614),(18446744073709551615)",
        "CREATE TABLE end_desc(k BIGINT)",
        "INSERT INTO end_desc VALUES (-1),(-9223372036854775808)",
        "CREATE TABLE needed_bound(k BIGINT)",
        "INSERT INTO needed_bound VALUES (2),(9223372036854775807)",
    ] {
        session.run(sql).unwrap();
    }
    for pipeline in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_pipelined_window_function={pipeline}"
            ))
            .unwrap();
        for (start, last_count) in [("UNBOUNDED PRECEDING", "2"), ("CURRENT ROW", "1")] {
            // The first upper bound reaches the partition end. Go never
            // evaluates the second row's overflowing addition afterward.
            let sql = format!(
                "SELECT k,COUNT(*) OVER (ORDER BY k RANGE BETWEEN {start} AND \
                 9223372036854775806 FOLLOWING) FROM t ORDER BY k"
            );
            assert_eq!(
                row_text(session.run(&sql)),
                [["1", "2"], ["9223372036854775807", last_count]],
                "pipeline={pipeline}, {start}"
            );
        }
        for (sql, expected) in [
            (
                "SELECT k,COUNT(*) OVER (ORDER BY k RANGE BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) FROM start_signed ORDER BY k",
                [["9223372036854775805", "0"], ["9223372036854775806", "0"]],
            ),
            (
                "SELECT k,COUNT(*) OVER (ORDER BY k RANGE BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) FROM start_unsigned ORDER BY k",
                [["18446744073709551613", "0"], ["18446744073709551614", "0"]],
            ),
            (
                "SELECT k,COUNT(*) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM end_unsigned ORDER BY k",
                [["18446744073709551614", "2"], ["18446744073709551615", "1"]],
            ),
            (
                "SELECT k,COUNT(*) OVER (ORDER BY k DESC RANGE BETWEEN CURRENT ROW AND 9223372036854775807 FOLLOWING) FROM end_desc ORDER BY k DESC",
                [["-1", "2"], ["-9223372036854775808", "1"]],
            ),
        ] {
            assert_eq!(row_text(session.run(sql)), expected, "pipeline={pipeline}, {sql}");
        }
        // A boundary that is still needed must continue to report overflow.
        let error = session.run(
            "SELECT COUNT(*) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND 9223372036854775806 FOLLOWING) FROM needed_bound"
        ).unwrap_err().to_mysql_error();
        assert_eq!(error.code, 1690, "pipeline={pipeline}");
        assert_eq!(error.state, *b"22003");
    }
}

#[test]
fn window_range_boundary_warning_evaluation_matches_go() {
    let mut session = Session::new();
    session.run("CREATE TABLE t(k DATETIME)").unwrap();
    session
        .run("INSERT INTO t VALUES ('9999-12-30'),('9999-12-31'),('9999-12-31')")
        .unwrap();
    for pipeline in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_pipelined_window_function={pipeline}"
            ))
            .unwrap();
        for days in [1, 2] {
            let sql = format!(
                "SELECT k,COUNT(*) OVER (ORDER BY k RANGE BETWEEN CURRENT ROW AND \
                 INTERVAL {days} DAY FOLLOWING) FROM t ORDER BY k"
            );
            let counts = if days == 1 { ["3", "2", "2"] } else { ["0"; 3] };
            assert_eq!(
                row_text(session.run(&sql)),
                [
                    ["9999-12-30 00:00:00", counts[0]],
                    ["9999-12-31 00:00:00", counts[1]],
                    ["9999-12-31 00:00:00", counts[2]],
                ],
                "pipeline={pipeline}, days={days}"
            );
            let warnings = row_text(session.run("SHOW WARNINGS"));
            // The one-day bound exhausts the cursor before any overflow.
            // Go's pipelined lookahead evaluates a needed bound twice.
            let count = if days == 1 {
                0
            } else if pipeline == 0 {
                3
            } else {
                6
            };
            assert_eq!(
                warnings,
                vec![
                    [
                        "Warning",
                        "1441",
                        "Datetime function: datetime field overflow"
                    ];
                    count
                ],
                "pipeline={pipeline}, days={days}"
            );
        }
    }
}

/// `RANGE BETWEEN N PRECEDING/FOLLOWING`: the boundary is a VALUE of the
/// single `ORDER BY` key, so ties share a frame and a gap in the key
/// SHRINKS the frame rather than shifting it.
///
/// Every expectation is captured TiDB output over `k = 1,3,3,7,8`.
#[test]
fn window_range_value_bounds() {
    let mut session = range_session();

    for (frame, expected) in [
        (
            "RANGE BETWEEN 2 PRECEDING AND CURRENT ROW",
            [
                ["10", "1"],
                ["60", "3"],
                ["60", "3"],
                ["40", "1"],
                ["90", "2"],
            ],
        ),
        (
            "RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING",
            [
                ["10", "1"],
                ["50", "2"],
                ["50", "2"],
                ["90", "2"],
                ["90", "2"],
            ],
        ),
        (
            "RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING",
            [
                ["60", "3"],
                ["50", "2"],
                ["50", "2"],
                ["90", "2"],
                ["50", "1"],
            ],
        ),
        // An empty frame: SUM is NULL but COUNT is 0, as under ROWS.
        (
            "RANGE BETWEEN 1 FOLLOWING AND 2 FOLLOWING",
            [
                ["50", "2"],
                ["NULL", "0"],
                ["NULL", "0"],
                ["50", "1"],
                ["NULL", "0"],
            ],
        ),
        (
            "RANGE BETWEEN 2 PRECEDING AND 1 PRECEDING",
            [
                ["NULL", "0"],
                ["10", "1"],
                ["10", "1"],
                ["NULL", "0"],
                ["40", "1"],
            ],
        ),
        (
            "RANGE BETWEEN UNBOUNDED PRECEDING AND 1 FOLLOWING",
            [
                ["10", "1"],
                ["60", "3"],
                ["60", "3"],
                ["150", "5"],
                ["150", "5"],
            ],
        ),
        (
            "RANGE BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING",
            [
                ["150", "5"],
                ["140", "4"],
                ["140", "4"],
                ["90", "2"],
                ["90", "2"],
            ],
        ),
        // A zero-width value frame is still the whole PEER group.
        (
            "RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING",
            [
                ["10", "1"],
                ["50", "2"],
                ["50", "2"],
                ["40", "1"],
                ["50", "1"],
            ],
        ),
    ] {
        assert_eq!(
            row_text(session.run(&format!(
                "SELECT SUM(v) OVER (ORDER BY k {frame}) s, \
                     COUNT(*) OVER (ORDER BY k {frame}) c FROM ri"
            ))),
            expected,
            "frame {frame}"
        );
    }

    // A fractional offset is legal under RANGE (only ROWS demands an
    // integer): `1.5 PRECEDING` over `1,3,3,7,8` is `10,50,50,40,90`.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1.5 PRECEDING AND CURRENT ROW) \
                 FROM ri"
        )),
        [["10"], ["50"], ["50"], ["40"], ["90"]]
    );

    // The frame is per PARTITION, as everywhere else.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM ri"
        )),
        [["10"], ["50"], ["50"], ["90"], ["90"]]
    );

    // A DECIMAL key uses decimal arithmetic for the boundary value.
    session
        .run("CREATE TABLE rd (k DECIMAL(10,2), v BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO rd VALUES (1.00,1),(1.50,2),(2.25,3),(5.00,4)")
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 0.5e0 PRECEDING AND CURRENT ROW) FROM rd"
        )),
        [["1"], ["3"], ["3"], ["4"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM rd"
        )),
        [["1"], ["3"], ["5"], ["4"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 0.5 PRECEDING AND 0.5 FOLLOWING) \
                 FROM rd"
        )),
        [["3"], ["3"], ["3"], ["4"]]
    );
}

/// `RANGE` under `DESC`, and `RANGE` over NULL keys -- the two rules a
/// positional reading of the frame gets wrong.
#[test]
fn window_range_desc_direction_and_nulls() {
    let mut session = range_session();

    // Under DESC, `N PRECEDING` reaches the LARGER keys (the ones that
    // sort EARLIER), so at `k = 7` the frame is `{8, 7}` and not `{7, 3}`.
    // Go's Window emits its required child property, so rows come out in
    // window order `8,7,3,3,1` when there is no outer `ORDER BY`.
    assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) s, \
                 COUNT(*) OVER (ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) c FROM ri"
            )),
            [["50", "1"], ["90", "2"], ["50", "2"], ["50", "2"], ["60", "3"]]
        );
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) \
                 FROM ri"
        )),
        [["90"], ["40"], ["60"], ["60"], ["10"]]
    );

    // NULL keys form a frame of their OWN: they peer with each other and
    // with nothing else, so the two NULL rows see only each other (sum 3,
    // count 2) and no non-NULL row ever includes them.
    session.run("CREATE TABLE rn (k BIGINT, v BIGINT)").unwrap();
    session
        .run("INSERT INTO rn VALUES (NULL,1),(NULL,2),(1,10),(2,20),(5,50)")
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) s, \
                 COUNT(*) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) c FROM rn"
        )),
        [
            ["3", "2"],
            ["3", "2"],
            ["30", "2"],
            ["30", "2"],
            ["50", "1"]
        ]
    );
    // Under DESC the NULLs sort LAST, and still frame only each other.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) \
                 FROM rn"
        )),
        [["50"], ["30"], ["30"], ["3"], ["3"]]
    );
}

/// The DEFAULT frame, which is the single biggest divergence trap in
/// window functions: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`,
/// whose `CURRENT ROW` is PEER-INCLUSIVE.
///
/// Every expectation is captured TiDB output over the fixture.
#[test]
fn window_default_frame_includes_every_peer() {
    let mut session = window_session();

    // The tied 20s BOTH show 50 -- the running sum that already includes
    // both of them -- and neither shows 30. A row-by-row running total
    // would print 30 then 50, which is the classic wrong answer.
    assert_eq!(
        row_text(
            session.run(
                "SELECT g, v, SUM(v) OVER (PARTITION BY g ORDER BY v) AS s FROM t ORDER BY g, v"
            )
        ),
        [
            ["1", "10", "10"],
            ["1", "20", "50"],
            ["1", "20", "50"],
            ["1", "30", "80"],
            ["1", "40", "120"],
            ["2", "5", "10"],
            ["2", "5", "10"],
            ["2", "7", "17"],
        ]
    );

    // COUNT and AVG see the same peer-inclusive frame.
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, COUNT(v) OVER (PARTITION BY g ORDER BY v) AS c FROM t ORDER BY g, v"
        )),
        [
            ["1", "10", "1"],
            ["1", "20", "3"],
            ["1", "20", "3"],
            ["1", "30", "4"],
            ["1", "40", "5"],
            ["2", "5", "2"],
            ["2", "5", "2"],
            ["2", "7", "3"],
        ]
    );
    assert_eq!(
        row_text(
            session.run(
                "SELECT g, v, AVG(v) OVER (PARTITION BY g ORDER BY v) AS a FROM t ORDER BY g, v"
            )
        ),
        [
            ["1", "10", "10.0000"],
            ["1", "20", "16.6667"],
            ["1", "20", "16.6667"],
            ["1", "30", "20.0000"],
            ["1", "40", "24.0000"],
            ["2", "5", "5.0000"],
            ["2", "5", "5.0000"],
            ["2", "7", "5.6667"],
        ]
    );

    // Writing the default frame out by hand is the same frame.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING \
                 AND CURRENT ROW) AS s FROM t ORDER BY g, v"
        )),
        [
            ["10"],
            ["50"],
            ["50"],
            ["80"],
            ["120"],
            ["10"],
            ["10"],
            ["17"]
        ]
    );
    // ... and its mirror image runs the peers the other way.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v RANGE BETWEEN CURRENT ROW AND \
                 UNBOUNDED FOLLOWING) AS s FROM t ORDER BY g, v"
        )),
        [
            ["120"],
            ["110"],
            ["110"],
            ["70"],
            ["40"],
            ["17"],
            ["17"],
            ["7"]
        ]
    );

    // DESC only reverses the order the peers are walked in.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v DESC) AS s FROM t \
                 ORDER BY g, v DESC"
        )),
        [
            ["40"],
            ["70"],
            ["110"],
            ["110"],
            ["120"],
            ["7"],
            ["17"],
            ["17"]
        ]
    );

    // With NO window ORDER BY every row is a peer, so the frame is the
    // whole partition and every row shows the partition total.
    assert_eq!(
        row_text(session.run(
            "SELECT g, SUM(v) OVER (PARTITION BY g) AS s, COUNT(*) OVER (PARTITION BY g) AS c \
                 FROM t ORDER BY g, v"
        )),
        [
            ["1", "120", "5"],
            ["1", "120", "5"],
            ["1", "120", "5"],
            ["1", "120", "5"],
            ["1", "120", "5"],
            ["2", "17", "3"],
            ["2", "17", "3"],
            ["2", "17", "3"],
        ]
    );
    // No PARTITION BY either: the whole result set is one frame.
    assert_eq!(
        row_text(session.run("SELECT SUM(v) OVER () AS s FROM t ORDER BY g, v"))[0],
        ["137"]
    );
}

/// Explicit `ROWS BETWEEN` frames, including the ones that EXCLUDE the
/// current row and so leave some rows with an empty frame.
#[test]
fn window_rows_frames_and_the_empty_frame() {
    let mut session = window_session();

    // A sliding window: unlike the default RANGE frame, ROWS gives the
    // two tied 20s DIFFERENT sums (30 and 40), because it counts physical
    // positions rather than peers.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1 PRECEDING AND \
                 CURRENT ROW) AS s FROM t ORDER BY g, v"
        )),
        [
            ["10"],
            ["30"],
            ["40"],
            ["50"],
            ["70"],
            ["5"],
            ["10"],
            ["12"]
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND \
                 1 FOLLOWING) AS s FROM t ORDER BY g, v"
        )),
        [
            ["30"],
            ["40"],
            ["50"],
            ["70"],
            ["40"],
            ["10"],
            ["12"],
            ["7"]
        ]
    );
    // The unbounded ends, which clamp rather than error at the edges.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED PRECEDING \
                 AND UNBOUNDED FOLLOWING) AS s FROM t ORDER BY g, v"
        )),
        [
            ["120"],
            ["120"],
            ["120"],
            ["120"],
            ["120"],
            ["17"],
            ["17"],
            ["17"]
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND \
                 UNBOUNDED FOLLOWING) AS s FROM t ORDER BY g, v"
        )),
        [
            ["120"],
            ["110"],
            ["90"],
            ["70"],
            ["40"],
            ["17"],
            ["12"],
            ["7"]
        ]
    );

    // A frame that EXCLUDES the current row. The first row of each
    // partition has an EMPTY frame, and an empty frame is NULL for SUM
    // but ZERO for COUNT -- captured, and the trap this test exists for.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 PRECEDING AND \
                 1 PRECEDING) AS s FROM t ORDER BY g, v"
        )),
        [
            ["NULL"],
            ["10"],
            ["30"],
            ["40"],
            ["50"],
            ["NULL"],
            ["5"],
            ["10"],
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 PRECEDING AND \
                 1 PRECEDING) AS c FROM t ORDER BY g, v"
        )),
        [["0"], ["1"], ["2"], ["2"], ["2"], ["0"], ["1"], ["2"]]
    );

    // `2 FOLLOWING AND 1 FOLLOWING` is empty for EVERY row -- not a
    // static error, just an all-NULL column (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 FOLLOWING AND \
                 1 FOLLOWING) AS s FROM t ORDER BY g, v"
        )),
        [["NULL"]; 8]
    );
}

/// NULL inputs and string arguments across the framed families.
///
/// A window `ORDER BY` sorts NULLs FIRST ascending, and all NULL keys are
/// peers -- so the NULL row's own default frame holds only itself.
#[test]
fn window_frames_over_nulls_and_strings() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE u (g BIGINT, v BIGINT, s VARCHAR(20))")
        .unwrap();
    session
        .run("INSERT INTO u VALUES (1,10,'a'),(1,NULL,'b'),(1,20,NULL),(2,5,'x')")
        .unwrap();

    // The NULL row sorts first; SUM over its lone-NULL frame is NULL,
    // COUNT(v) is 0 while COUNT(*) is 1.
    assert_eq!(
        row_text(session.run(
            "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY v) AS s, \
                 COUNT(v) OVER (PARTITION BY g ORDER BY v) AS c, \
                 COUNT(*) OVER (PARTITION BY g ORDER BY v) AS ca \
                 FROM u ORDER BY g, v"
        )),
        [
            ["NULL", "NULL", "0", "1"],
            ["10", "10", "1", "2"],
            ["20", "30", "2", "3"],
            ["5", "5", "1", "1"],
        ]
    );

    // FIRST_VALUE reads the frame's first ROW, NULL included -- it does
    // not skip to the first non-NULL value.
    assert_eq!(
        row_text(session.run(
            "SELECT FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY v) AS f FROM u ORDER BY g, v"
        )),
        [["NULL"], ["NULL"], ["NULL"], ["5"]]
    );

    // MIN/MAX over strings SKIP NULLs, as in ordinary aggregation.
    assert_eq!(
        row_text(session.run(
            "SELECT MIN(s) OVER (PARTITION BY g) AS lo, MAX(s) OVER (PARTITION BY g) AS hi \
                 FROM u ORDER BY g, v"
        )),
        [["a", "b"], ["a", "b"], ["a", "b"], ["x", "x"]]
    );

    // A string LAG default lands on the partition's first row.
    assert_eq!(
        row_text(session.run(
            "SELECT LAG(s, 1, 'zz') OVER (PARTITION BY g ORDER BY v) AS l FROM u \
                 ORDER BY g, v"
        )),
        [["zz"], ["b"], ["a"], ["zz"]]
    );
}

/// `RANGE BETWEEN INTERVAL n unit PRECEDING/FOLLOWING` over a temporal
/// `ORDER BY` key: the boundary is the current row's key moved by
/// `DATE_ADD`/`DATE_SUB`'s own CALENDAR arithmetic, so `INTERVAL 1 MONTH`
/// is a month field increment rather than a fixed number of days, and the
/// boundary is INCLUSIVE.
///
/// Every expectation is captured TiDB output.
#[test]
fn window_range_interval_bounds() {
    let mut session = interval_session();
    let error = session.run(
        "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN INTERVAL '-1 2' DAY_HOUR PRECEDING AND CURRENT ROW) FROM td"
    ).unwrap_err().to_mysql_error();
    assert_eq!(error.code, 3586);
    assert_eq!(
        error.message,
        "Window '<unnamed window>': frame start or end is negative, NULL or of non-integral type"
    );

    // Captured: the `2020-01-02` rows see the whole day back to
    // `2020-01-01 00:00:00` INCLUSIVE (10+20+30+40 = 100), the tie shares
    // one frame, and the `2020-01-05` row's window reaches nothing.
    for sql in [
        "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
             RANGE INTERVAL 1 DAY PRECEDING) FROM td WHERE g = 1 ORDER BY k, v",
        "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
             RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM td \
             WHERE g = 1 ORDER BY k, v",
    ] {
        assert_eq!(
            row_text(session.run(sql)),
            [
                ["10", "10"],
                ["20", "30"],
                ["30", "100"],
                ["40", "100"],
                ["50", "50"]
            ],
            "for {sql}"
        );
    }

    // Captured: `CURRENT ROW AND INTERVAL 1 DAY FOLLOWING` looks forward
    // over the same inclusive boundary.
    assert_eq!(
        row_text(session.run(
            "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN CURRENT ROW AND INTERVAL 1 DAY FOLLOWING) FROM td \
                 WHERE g = 1 ORDER BY k, v"
        )),
        [
            ["10", "100"],
            ["20", "90"],
            ["30", "70"],
            ["40", "70"],
            ["50", "50"]
        ]
    );

    // Captured: a two-sided interval frame, and a 2 HOUR step that
    // reaches NOTHING but the peer group for the first two rows.
    assert_eq!(
        row_text(session.run(
            "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 1 DAY FOLLOWING) \
                 FROM td WHERE g = 1 ORDER BY k, v"
        )),
        [
            ["10", "100"],
            ["20", "100"],
            ["30", "100"],
            ["40", "100"],
            ["50", "50"]
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN INTERVAL 2 HOUR PRECEDING AND INTERVAL 2 HOUR FOLLOWING) \
                 FROM td WHERE g = 1 ORDER BY k, v"
        )),
        [
            ["10", "10"],
            ["20", "20"],
            ["30", "70"],
            ["40", "70"],
            ["50", "50"]
        ]
    );

    // Captured: under DESC the sign FLIPS, so `INTERVAL 1 DAY PRECEDING`
    // reaches the LATER timestamps that sort earlier.
    assert_eq!(
        row_text(session.run(
            "SELECT v, SUM(v) OVER (PARTITION BY g ORDER BY k DESC \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM td \
                 WHERE g = 1 ORDER BY k DESC, v"
        )),
        [
            ["50", "50"],
            ["30", "70"],
            ["40", "70"],
            ["20", "90"],
            ["10", "100"]
        ]
    );

    // COUNT counts the same frame (captured `1,2,4,4,1`), and
    // FIRST_VALUE reads it.
    assert_eq!(
        row_text(session.run(
            "SELECT COUNT(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM td \
                 WHERE g = 1 ORDER BY k, v"
        )),
        [["1"], ["2"], ["4"], ["4"], ["1"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 1 DAY FOLLOWING) \
                 FROM td WHERE g = 1 ORDER BY k, v"
        )),
        [["10"], ["10"], ["10"], ["10"], ["50"]]
    );

    // Captured: MONTH and YEAR reach every row of the whole table, and a
    // composite `INTERVAL '1 2' DAY_HOUR` (26 hours) reaches back far
    // enough for the `2020-01-02` rows but not for `2020-01-05`.
    for sql in [
        "SELECT v, SUM(v) OVER (ORDER BY k \
             RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND CURRENT ROW) FROM td ORDER BY k, v",
        "SELECT v, SUM(v) OVER (ORDER BY k \
             RANGE BETWEEN INTERVAL 1 YEAR PRECEDING AND CURRENT ROW) FROM td ORDER BY k, v",
    ] {
        assert_eq!(
            row_text(session.run(sql)),
            [
                ["10", "70"],
                ["60", "70"],
                ["20", "90"],
                ["30", "160"],
                ["40", "160"],
                ["50", "210"]
            ],
            "for {sql}"
        );
    }
    assert_eq!(
        row_text(session.run(
            "SELECT v, SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL '1 2' DAY_HOUR PRECEDING AND CURRENT ROW) \
                 FROM td ORDER BY k, v"
        )),
        [
            ["10", "70"],
            ["60", "70"],
            ["20", "90"],
            ["30", "160"],
            ["40", "160"],
            ["50", "50"]
        ]
    );

    // An interval frame whose start ranks after its end is EMPTY for
    // every row (captured: all NULL).
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND INTERVAL 2 DAY PRECEDING) \
                 FROM td ORDER BY k, v"
        )),
        [["NULL"], ["NULL"], ["NULL"], ["NULL"], ["NULL"], ["NULL"]]
    );
}

/// The same interval frame over the OTHER temporal key types, and over
/// NULL keys -- which peer with each other and with nothing else.
#[test]
fn window_range_interval_over_dates_and_nulls() {
    let mut session = Session::new();

    // Captured over `NULL,NULL,'2020-01-01','2020-01-02'` with values
    // `1,2,3,4`: the two NULL keys form a frame of their own (3 = 1+2),
    // in BOTH directions.
    session
        .run("CREATE TABLE tdn (k DATETIME, v BIGINT)")
        .unwrap();
    session
        .run(
            "INSERT INTO tdn VALUES \
                 (NULL,1),(NULL,2),('2020-01-01 00:00:00',3),('2020-01-02 00:00:00',4)",
        )
        .unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM tdn"
        )),
        [["3"], ["3"], ["3"], ["7"]]
    );
    // Under DESC the `2020-01-01` row's frame reaches FORWARD to
    // `2020-01-02` (3+4 = 7); the NULL rows still see only each other.
    // Ordered by `v` because a window's own sort is not an output order.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k DESC \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM tdn ORDER BY v"
        )),
        [["3"], ["3"], ["7"], ["4"]]
    );

    // A DATE key reads as midnight, so a 2 HOUR step reaches nothing
    // outside the peer group while a 1 DAY step reaches the previous day
    // (captured `1,6,6,4` and `1,5,5,4`).
    session
        .run("CREATE TABLE tdate (k DATE, v BIGINT)")
        .unwrap();
    session
            .run("INSERT INTO tdate VALUES ('2020-01-01',1),('2020-01-02',2),('2020-01-02',3),('2020-01-10',4)")
            .unwrap();
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM tdate"
        )),
        [["1"], ["6"], ["6"], ["4"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 2 HOUR PRECEDING AND CURRENT ROW) FROM tdate"
        )),
        [["1"], ["5"], ["5"], ["4"]]
    );
    // A month either side reaches every row (captured `10` throughout).
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k \
                 RANGE BETWEEN INTERVAL 1 MONTH PRECEDING AND INTERVAL 1 MONTH FOLLOWING) \
                 FROM tdate"
        )),
        [["10"], ["10"], ["10"], ["10"]]
    );
}

/// Captured from Go's ordinary and pipelined executors: their unsigned
/// FOLLOWING end arithmetic differs because clamping occurs at different times.
#[test]
fn window_unsigned_rows_bounds_match_go_execution_mode() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (id INT, v INT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    for pipelined in [0, 1] {
        session
            .run(&format!(
                "SET tidb_enable_pipelined_window_function={pipelined}"
            ))
            .unwrap();
        for (bounds, expected) in [
            (
                "CURRENT ROW AND 18446744073709551615 FOLLOWING",
                if pipelined == 0 {
                    [["10"], ["NULL"], ["NULL"]]
                } else {
                    [["NULL"], ["NULL"], ["NULL"]]
                },
            ),
            (
                "18446744073709551615 FOLLOWING AND UNBOUNDED FOLLOWING",
                [["NULL"], ["10"], ["20"]],
            ),
            (
                "18446744073709551615 PRECEDING AND CURRENT ROW",
                [["10"], ["10"], ["10"]],
            ),
        ] {
            let sql =
                format!("SELECT FIRST_VALUE(v) OVER (ORDER BY id ROWS BETWEEN {bounds}) FROM t");
            assert_eq!(
                row_text(session.run(&sql)),
                expected,
                "pipelined={pipelined}: {sql}"
            );
        }
    }
}

/// Go recovers getRows' unsigned slice-bound panic when a wrapped FOLLOWING
/// frame reaches a released input chunk. Rust returns that error without panic.
#[test]
fn window_wrapped_frame_released_input_returns_go_error() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (id INT, v INT)").unwrap();
    session
        .run("INSERT INTO t VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    session
        .run("SET tidb_enable_pipelined_window_function=1")
        .unwrap();
    session
        .vars
        .restore_system(vec![("tidb_max_chunk_size".into(), Some("1".into()))]);
    let error = session.run("SELECT FIRST_VALUE(v) OVER (ORDER BY id ROWS BETWEEN 18446744073709551615 FOLLOWING AND UNBOUNDED FOLLOWING) FROM t").unwrap_err().to_mysql_error();
    assert_eq!(error.code, 1105);
    assert_eq!(
        error.message,
        "runtime error: slice bounds out of range [18446744073709551615:2]"
    );
}

/// Results and recovered errors verified against the Go window executor at
/// chunk sizes 1, 2, and 1024, in both execution and precision modes.
#[test]
fn window_wrapped_sliding_end_preserves_go_error() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (id INT, v INT, f DOUBLE)")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,10,10),(2,20,20),(3,30,30)")
        .unwrap();
    for chunk_size in [1, 2, 1024] {
        // Match Go's direct SessionVars.MaxChunkSize assignment; the SQL SET
        // validator intentionally rejects the tiny sizes used by its tests.
        session.vars.restore_system(vec![(
            "tidb_max_chunk_size".into(),
            Some(chunk_size.to_string()),
        )]);
        for precision in [0, 1] {
            session
                .run(&format!("SET windowing_use_high_precision={precision}"))
                .unwrap();
            for pipelined in [0, 1] {
                session
                    .run(&format!(
                        "SET tidb_enable_pipelined_window_function={pipelined}"
                    ))
                    .unwrap();
                for (function, empty, whole, suffix) in [
                    ("SUM(v)", "NULL", "60", "50"),
                    ("AVG(v)", "NULL", "20.0000", "25.0000"),
                    ("SUM(f)", "NULL", "60", "50"),
                    ("AVG(f)", "NULL", "20", "25"),
                    ("COUNT(v)", "0", "3", "2"),
                    ("MIN(v)", "NULL", "10", "20"),
                    ("MAX(v)", "NULL", "30", "30"),
                    ("BIT_XOR(v)", "0", "0", "10"),
                ] {
                    for following_start in [false, true] {
                        let bounds = if following_start {
                            "18446744073709551615 FOLLOWING AND UNBOUNDED FOLLOWING"
                        } else {
                            "CURRENT ROW AND 18446744073709551615 FOLLOWING"
                        };
                        let sql = format!(
                            "SELECT {function} OVER (ORDER BY id ROWS BETWEEN {bounds}) FROM t"
                        );
                        let context = format!(
                            "chunk={chunk_size} precision={precision} pipelined={pipelined}: {sql}"
                        );
                        let error = if following_start && pipelined == 1 && chunk_size == 1 {
                            Some(
                                "runtime error: slice bounds out of range [18446744073709551615:2]",
                            )
                        } else if !following_start
                            && pipelined == 0
                            && chunk_size > 1
                            && !(precision == 1 && function.ends_with("(f)"))
                        {
                            Some("runtime error: index out of range [3] with length 3")
                        } else {
                            None
                        };
                        if let Some(expected) = error {
                            let error = session.run(&sql).unwrap_err().to_mysql_error();
                            assert_eq!(error.code, 1105, "{context}");
                            assert_eq!(error.message, expected, "{context}");
                        } else {
                            let expected = if following_start {
                                [[empty], [whole], [suffix]]
                            } else if pipelined == 0 {
                                [[whole], [empty], [empty]]
                            } else {
                                [[empty], [empty], [empty]]
                            };
                            assert_eq!(row_text(session.run(&sql)), expected, "{context}");
                        }
                    }
                }
            }
        }
    }
}
