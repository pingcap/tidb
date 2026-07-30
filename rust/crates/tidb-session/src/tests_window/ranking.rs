//! Ranking window functions: `RANK`, `DENSE_RANK`, `ROW_NUMBER`, `NTILE`,
//! `PERCENT_RANK` and `CUME_DIST`, all of which read the peer geometry Go
//! computes in `pkg/executor/aggregate`'s window path rather than a frame.

use crate::tests_support::*;
use crate::*;

/// `ROW_NUMBER`/`RANK`/`DENSE_RANK` over ties, checked against captured
/// TiDB output.
///
/// The three differ only on peers: `ROW_NUMBER` numbers every row,
/// `RANK` gives peers the same rank and then SKIPS to the next row's
/// 1-based position, `DENSE_RANK` gives peers the same rank and never
/// skips.
#[test]
fn window_ranking_functions_over_ties() {
    let mut session = window_session();

    assert_eq!(
        row_text(session.run(
            "SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn \
                 FROM t ORDER BY g, v, rn"
        )),
        [
            ["1", "10", "1"],
            ["1", "20", "2"],
            ["1", "20", "3"],
            ["1", "30", "4"],
            ["1", "40", "5"],
            ["2", "5", "1"],
            ["2", "5", "2"],
            ["2", "7", "3"],
        ]
    );

    // Captured: the tied 20s both rank 2, and 30 jumps to 4.
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, RANK() OVER (PARTITION BY g ORDER BY v) AS r \
                 FROM t ORDER BY g, v"
        )),
        [
            ["1", "10", "1"],
            ["1", "20", "2"],
            ["1", "20", "2"],
            ["1", "30", "4"],
            ["1", "40", "5"],
            ["2", "5", "1"],
            ["2", "5", "1"],
            ["2", "7", "3"],
        ]
    );

    // Captured: the same ties, but 30 is 3, not 4.
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, DENSE_RANK() OVER (PARTITION BY g ORDER BY v) AS r \
                 FROM t ORDER BY g, v"
        )),
        [
            ["1", "10", "1"],
            ["1", "20", "2"],
            ["1", "20", "2"],
            ["1", "30", "3"],
            ["1", "40", "4"],
            ["2", "5", "1"],
            ["2", "5", "1"],
            ["2", "7", "2"],
        ]
    );

    // No window ORDER BY at all: every row of the partition is a peer,
    // so both rank functions return 1 for all of them (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT g, RANK() OVER (PARTITION BY g) AS r, \
                 DENSE_RANK() OVER (PARTITION BY g) AS d FROM t ORDER BY g, r, d"
        )),
        [
            ["1", "1", "1"],
            ["1", "1", "1"],
            ["1", "1", "1"],
            ["1", "1", "1"],
            ["1", "1", "1"],
            ["2", "1", "1"],
            ["2", "1", "1"],
            ["2", "1", "1"],
        ]
    );

    // DESC reverses the window's own order, independently of the outer
    // ORDER BY (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v DESC) AS rn \
                 FROM t WHERE g = 2 ORDER BY rn"
        )),
        [["2", "7", "1"], ["2", "5", "2"], ["2", "5", "3"]]
    );
}

/// `NTILE(n)`'s bucket sizing, checked against captured TiDB output.
///
/// With `n` buckets over `rows` rows the FIRST `rows % n` buckets take
/// one extra row (`quotient + 1`) and the rest take `quotient`; when
/// `n > rows` the surplus buckets stay empty.
#[test]
fn window_ntile_bucket_distribution() {
    let mut session = window_session();

    // 5 rows into 2 buckets -> 3 then 2; 3 rows into 2 -> 2 then 1.
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, NTILE(2) OVER (PARTITION BY g ORDER BY v) AS b \
                 FROM t ORDER BY g, v"
        )),
        [
            ["1", "10", "1"],
            ["1", "20", "1"],
            ["1", "20", "1"],
            ["1", "30", "2"],
            ["1", "40", "2"],
            ["2", "5", "1"],
            ["2", "5", "1"],
            ["2", "7", "2"],
        ]
    );

    // 5 rows into 3 buckets -> 2, 2, 1 (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT v, NTILE(3) OVER (PARTITION BY g ORDER BY v) AS b \
                 FROM t WHERE g = 1 ORDER BY v"
        )),
        [
            ["10", "1"],
            ["20", "1"],
            ["20", "2"],
            ["30", "2"],
            ["40", "3"]
        ]
    );

    // More buckets than rows: one row each, the rest empty (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT v, NTILE(5) OVER (PARTITION BY g ORDER BY v) AS b \
                 FROM t WHERE g = 2 ORDER BY b"
        )),
        [["5", "1"], ["5", "2"], ["7", "3"]]
    );

    // One bucket holds everything (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT NTILE(1) OVER (PARTITION BY g ORDER BY v) AS b \
                 FROM t WHERE g = 2 ORDER BY b"
        )),
        [["1"], ["1"], ["1"]]
    );

    // Without PARTITION BY the whole result is one partition: 8 rows
    // into 2 buckets -> 4 then 4 (captured).
    assert_eq!(
        row_text(session.run("SELECT v, NTILE(2) OVER (ORDER BY v) AS b FROM t ORDER BY v")),
        [
            ["5", "1"],
            ["5", "1"],
            ["7", "1"],
            ["10", "1"],
            ["20", "2"],
            ["20", "2"],
            ["30", "2"],
            ["40", "2"],
        ]
    );
}

/// `PERCENT_RANK()` and `CUME_DIST()`: both are PEER-based, both ignore
/// the frame, and `PERCENT_RANK` answers `0` rather than NaN when the
/// partition holds a single row.
#[test]
fn window_percent_rank_and_cume_dist() {
    let mut session = Session::new();
    session.run("CREATE TABLE pr (g BIGINT, v BIGINT)").unwrap();
    session
        .run("INSERT INTO pr VALUES (1,10),(1,20),(1,20),(1,30),(2,5)")
        .unwrap();

    // Captured: the tied 20s SHARE both values, and the one-row partition
    // `g = 2` is PERCENT_RANK 0 / CUME_DIST 1.
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, PERCENT_RANK() OVER (PARTITION BY g ORDER BY v) p, \
                 CUME_DIST() OVER (PARTITION BY g ORDER BY v) c FROM pr ORDER BY g, v"
        )),
        [
            ["1", "10", "0", "0.25"],
            ["1", "20", "0.3333333333333333", "0.75"],
            ["1", "20", "0.3333333333333333", "0.75"],
            ["1", "30", "1", "1"],
            ["2", "5", "0", "1"],
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT PERCENT_RANK() OVER (ORDER BY v) p, CUME_DIST() OVER (ORDER BY v) c \
                 FROM pr ORDER BY v"
        )),
        [
            ["0", "0.2"],
            ["0.25", "0.4"],
            ["0.5", "0.8"],
            ["0.5", "0.8"],
            ["1", "1"],
        ]
    );
    // With NO ORDER BY every row is a peer: rank 1 everywhere, so
    // PERCENT_RANK is 0 and CUME_DIST is 1.
    assert_eq!(
        row_text(session.run("SELECT PERCENT_RANK() OVER () p, CUME_DIST() OVER () c FROM pr")),
        [["0", "1"], ["0", "1"], ["0", "1"], ["0", "1"], ["0", "1"],]
    );
    // DESC reverses which peer group is first.
    assert_eq!(
        row_text(session.run(
            "SELECT PERCENT_RANK() OVER (ORDER BY v DESC) p, \
                 CUME_DIST() OVER (ORDER BY v DESC) c FROM pr ORDER BY v DESC"
        )),
        [
            ["0", "0.2"],
            ["0.25", "0.6"],
            ["0.25", "0.6"],
            ["0.75", "0.8"],
            ["1", "1"],
        ]
    );
    // A written frame is IGNORED by both (the values match the frameless
    // form above), though it is still VALIDATED.
    assert_eq!(
        row_text(session.run(
            "SELECT PERCENT_RANK() OVER (ORDER BY v ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) \
                 FROM pr ORDER BY v"
        )),
        [["0"], ["0.25"], ["0.5"], ["0.5"], ["1"]]
    );
    assert!(matches!(
        session.run(
            "SELECT PERCENT_RANK() OVER (ORDER BY v ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) \
                 FROM pr"
        ),
        Err(DriverError::WindowFrameIllegal)
    ));

    // Both are a NOT NULL DOUBLE (Go's `typeInfer4PercentRank` /
    // `typeInfer4CumeDist`).
    for sql in [
        "SELECT PERCENT_RANK() OVER (ORDER BY v) FROM pr",
        "SELECT CUME_DIST() OVER (ORDER BY v) FROM pr",
    ] {
        match session.run_with_columns(sql).unwrap() {
            StmtOutput::Rows { columns, .. } => {
                let (_, ftype) = &columns[0];
                assert_eq!(ftype.code(), tidb_datatype::FieldTypeCode::Double, "{sql}");
                assert_ne!(ftype.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL, 0);
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }
}

/// The fixture the retired `tidb-exec` window vectors ran over, kept so
/// their re-captured expectations read against the same shape: one
/// four-row partition with a TIE, plus a one-row partition.
fn ranking_live_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ranking_live (id INT, p INT, k INT)")
        .unwrap();
    session
        .run("INSERT INTO ranking_live VALUES (1,1,10),(2,1,20),(3,1,20),(4,1,30),(5,2,7)")
        .unwrap();
    session
}

/// Every peer-aware window function in ONE query, so the shared peer
/// geometry is proved to be shared rather than nine independent walks.
///
/// Re-captured from real TiDB (`testkit.CreateMockStore`) over the
/// fixture above; the tie at `k = 20` is what separates the six ranking
/// columns from each other.
#[test]
fn window_all_ranking_consumers_share_one_peer_geometry() {
    let mut session = ranking_live_session();

    assert_eq!(
        row_text(session.run(
            "SELECT id, ROW_NUMBER() OVER w, RANK() OVER w, DENSE_RANK() OVER w, \
                 PERCENT_RANK() OVER w, CUME_DIST() OVER w, NTILE(3) OVER w, \
                 LAG(id) OVER w, LEAD(id) OVER w \
                 FROM ranking_live WINDOW w AS (PARTITION BY p ORDER BY k) ORDER BY id"
        )),
        [
            ["1", "1", "1", "1", "0", "0.25", "1", "NULL", "2"],
            [
                "2",
                "2",
                "2",
                "2",
                "0.3333333333333333",
                "0.75",
                "1",
                "1",
                "3"
            ],
            [
                "3",
                "3",
                "2",
                "2",
                "0.3333333333333333",
                "0.75",
                "2",
                "2",
                "4"
            ],
            ["4", "4", "4", "3", "1", "1", "3", "3", "NULL"],
            ["5", "1", "1", "1", "0", "1", "1", "NULL", "NULL"],
        ]
    );

    // With NO window ORDER BY the whole partition is one peer group:
    // ROW_NUMBER still counts physical rows, everything peer-aware
    // collapses to 1 / 1 / 0 / 1 (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT id, ROW_NUMBER() OVER w, RANK() OVER w, DENSE_RANK() OVER w, \
                 PERCENT_RANK() OVER w, CUME_DIST() OVER w \
                 FROM ranking_live WINDOW w AS (PARTITION BY p) ORDER BY id"
        )),
        [
            ["1", "1", "1", "1", "0", "1"],
            ["2", "2", "1", "1", "0", "1"],
            ["3", "3", "1", "1", "0", "1"],
            ["4", "4", "1", "1", "0", "1"],
            ["5", "1", "1", "1", "0", "1"],
        ]
    );
}

/// `NTILE`'s bucket count is resolved ONCE per descriptor from a
/// constant, through the same unsigned path as Go's
/// `GetUint64FromConstant` -- so the full positive `uint64` domain is
/// accepted, `TRUE` resolves to one, and `NULL` makes every row NULL
/// rather than erroring.
///
/// Re-captured from real TiDB. `window_ntile_bucket_distribution` covers
/// the bucket SIZING; this covers the ARGUMENT domain and the fact that
/// a partition with no window ORDER BY still buckets in scan order.
#[test]
fn window_ntile_argument_domain() {
    let mut session = ranking_live_session();

    // No window ORDER BY: the buckets follow the partition's scan order
    // and match the ordered form exactly (captured).
    for spec in ["PARTITION BY p", "PARTITION BY p ORDER BY k"] {
        assert_eq!(
            row_text(session.run(&format!(
                "SELECT id, NTILE(3) OVER ({spec}) FROM ranking_live ORDER BY id"
            ))),
            [["1", "1"], ["2", "1"], ["3", "2"], ["4", "3"], ["5", "1"]],
            "for {spec}"
        );
    }

    // More buckets than rows, and `uint64::MAX` buckets, distribute the
    // same way: one row each from the front (captured).
    for count in ["5", "18446744073709551615"] {
        assert_eq!(
            row_text(session.run(&format!(
                "SELECT id, NTILE({count}) OVER (PARTITION BY p ORDER BY k) \
                     FROM ranking_live ORDER BY id"
            ))),
            [["1", "1"], ["2", "2"], ["3", "3"], ["4", "4"], ["5", "1"]],
            "for NTILE({count})"
        );
    }

    // NULL is a legal constant: it yields NULL per row, not an error
    // (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT id, NTILE(NULL) OVER (PARTITION BY p ORDER BY k) \
                 FROM ranking_live ORDER BY id"
        )),
        [
            ["1", "NULL"],
            ["2", "NULL"],
            ["3", "NULL"],
            ["4", "NULL"],
            ["5", "NULL"],
        ]
    );

    // Captured: "[planner:1210]Incorrect arguments to ntile" for zero,
    // negative, FALSE (which is zero), and a row-dependent count.
    for sql in [
        "SELECT NTILE(0) OVER (ORDER BY id) FROM ranking_live",
        "SELECT NTILE(-1) OVER (ORDER BY id) FROM ranking_live",
        "SELECT NTILE(k) OVER (ORDER BY id) FROM ranking_live",
    ] {
        assert!(
            matches!(session.run(sql), Err(DriverError::WrongArguments("ntile"))),
            "expected ErrWrongArguments for {sql}"
        );
    }

    // `NTILE(FALSE)` must be rejected on the SAME ground as `NTILE(0)`:
    // `FALSE` lowers to the integer constant `0`, which fails the
    // positivity check, not the "not a constant at all" check a plain
    // column reference like `NTILE(k)` fails. Both grounds produce the
    // identical `DriverError::WrongArguments("ntile")` / errno 1210 /
    // "Incorrect arguments to ntile", so asserting only the error variant
    // cannot distinguish them -- `constant_uint`'s unit tests in
    // `tidb-executor/src/window.rs` (`constant_uint_tests`) pin the actual
    // ground: `FALSE` resolves to `Some(Some(0))`, exactly like `NTILE(0)`,
    // rather than `None`, like the non-constant `NTILE(k)`.
    let false_error = session
        .run("SELECT NTILE(FALSE) OVER (ORDER BY id) FROM ranking_live")
        .unwrap_err()
        .to_mysql_error();
    let zero_error = session
        .run("SELECT NTILE(0) OVER (ORDER BY id) FROM ranking_live")
        .unwrap_err()
        .to_mysql_error();
    assert_eq!(false_error, zero_error);
    assert_eq!(false_error.code, 1210);
    assert_eq!(false_error.message, "Incorrect arguments to ntile");
}

/// `TRUE` is an `Int64`-valued `Constant` in Go's expression layer, so
/// `NewWindowFuncDesc`'s `GetUint64FromConstant` reads it as ONE bucket;
/// captured from real TiDB, which returns bucket 1 for every row.
/// `constant_uint` (`tidb-executor/src/window.rs`) now folds boolean
/// literals into the same unsigned constant read Go uses (`TRUE` -> 1,
/// `FALSE` -> 0).
#[test]
fn window_ntile_accepts_boolean_constant() {
    let mut session = ranking_live_session();
    assert_eq!(
        row_text(session.run(
            "SELECT id, NTILE(TRUE) OVER (PARTITION BY p ORDER BY k) \
                 FROM ranking_live ORDER BY id"
        )),
        [["1", "1"], ["2", "1"], ["3", "1"], ["4", "1"], ["5", "1"]]
    );
}
