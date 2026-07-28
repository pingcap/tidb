#![cfg(test)]

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

/// The empty and partition-less specs, plus named windows, checked
/// against captured TiDB output.
#[test]
fn window_specs_and_named_windows() {
    let mut session = window_session();

    // `OVER ()`: one partition, no order -- the rows keep their source
    // order and are numbered through it (captured).
    assert_eq!(
        row_text(session.run("SELECT g, v, ROW_NUMBER() OVER () AS rn FROM t ORDER BY rn")),
        [
            ["1", "10", "1"],
            ["1", "20", "2"],
            ["1", "20", "3"],
            ["1", "30", "4"],
            ["1", "40", "5"],
            ["2", "5", "6"],
            ["2", "5", "7"],
            ["2", "7", "8"],
        ]
    );

    // No PARTITION BY, just an order: one partition across the table.
    assert_eq!(
        row_text(session.run("SELECT v, ROW_NUMBER() OVER (ORDER BY v) AS rn FROM t ORDER BY rn")),
        [
            ["5", "1"],
            ["5", "2"],
            ["7", "3"],
            ["10", "4"],
            ["20", "5"],
            ["20", "6"],
            ["30", "7"],
            ["40", "8"],
        ]
    );

    // One named window feeding two calls (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, ROW_NUMBER() OVER w AS rn, RANK() OVER w AS r \
                 FROM t WINDOW w AS (PARTITION BY g ORDER BY v) ORDER BY g, v, rn"
        )),
        [
            ["1", "10", "1", "1"],
            ["1", "20", "2", "2"],
            ["1", "20", "3", "2"],
            ["1", "30", "4", "4"],
            ["1", "40", "5", "5"],
            ["2", "5", "1", "1"],
            ["2", "5", "2", "1"],
            ["2", "7", "3", "3"],
        ]
    );

    // `OVER (w ...)`: a parenthesized reference may EXTEND the named
    // window with an ORDER BY the base does not have (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT v, ROW_NUMBER() OVER (w ORDER BY v) AS rn \
                 FROM t WHERE g = 2 WINDOW w AS (PARTITION BY g) ORDER BY rn"
        )),
        [["5", "1"], ["5", "2"], ["7", "3"]]
    );

    // A window function alongside plain columns and expressions
    // (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT g, v + 1, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn \
                 FROM t WHERE g = 2 ORDER BY rn"
        )),
        [["2", "6", "1"], ["2", "6", "2"], ["2", "8", "3"]]
    );
}

/// The outer `ORDER BY` runs AFTER the window is computed, checked
/// against captured TiDB output: the ranking reflects the WINDOW's order,
/// while the rows come out in the OUTER order.
#[test]
fn window_outer_order_by_applies_after_computation() {
    let mut session = window_session();

    assert_eq!(
        row_text(session.run(
            "SELECT v, g, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) AS rn \
                 FROM t ORDER BY v DESC, g, rn"
        )),
        [
            ["40", "1", "5"],
            ["30", "1", "4"],
            ["20", "1", "2"],
            ["20", "1", "3"],
            ["10", "1", "1"],
            ["7", "2", "3"],
            ["5", "2", "1"],
            ["5", "2", "2"],
        ]
    );

    // Ordering by the window column's POSITION works the same way
    // (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) \
                 FROM t ORDER BY 3 DESC, g"
        )),
        [
            ["1", "40", "5"],
            ["1", "30", "4"],
            ["1", "20", "3"],
            ["2", "7", "3"],
            ["1", "20", "2"],
            ["2", "5", "2"],
            ["1", "10", "1"],
            ["2", "5", "1"],
        ]
    );
}

/// The ranking functions' result types, checked against captured TiDB
/// metadata: `BIGINT(21)` for all four, `NOT NULL` for the three ranking
/// ones and `UNSIGNED`/binary for `NTILE`.
#[test]
fn window_result_types() {
    let mut session = window_session();

    match session
        .run_with_columns("SELECT ROW_NUMBER() OVER (ORDER BY v) FROM t")
        .unwrap()
    {
        StmtOutput::Rows { columns, .. } => {
            let (_, ftype) = &columns[0];
            assert_eq!(ftype.code(), tidb_datatype::FieldTypeCode::LongLong);
            assert_eq!(ftype.flen(), 21);
            assert!(!ftype.is_unsigned());
            assert_ne!(ftype.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL, 0);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    match session
        .run_with_columns("SELECT NTILE(2) OVER (ORDER BY v) FROM t")
        .unwrap()
    {
        StmtOutput::Rows { columns, .. } => {
            let (_, ftype) = &columns[0];
            assert_eq!(ftype.code(), tidb_datatype::FieldTypeCode::LongLong);
            assert_eq!(ftype.flen(), 21);
            assert!(ftype.is_unsigned());
            assert_eq!(ftype.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL, 0);
        }
        other => panic!("expected rows, got {other:?}"),
    }

    // The framed families' result TYPE CODES, captured over a `BIGINT v`:
    // an aggregate follows Go's `TypeInfer` (SUM/AVG are DECIMAL, COUNT a
    // NOT NULL BIGINT, MIN the argument's own type), and the value family
    // plus a defaultless LAG carry the argument's type. Go's display
    // WIDTHS on top (`DECIMAL(41,0)`, `DECIMAL(24,4)`) are the same
    // documented deferral the GROUP BY path this stage shares already has.
    use tidb_datatype::FieldTypeCode;
    for (sql, code) in [
        ("SELECT SUM(v) OVER () FROM t", FieldTypeCode::NewDecimal),
        ("SELECT AVG(v) OVER () FROM t", FieldTypeCode::NewDecimal),
        ("SELECT COUNT(v) OVER () FROM t", FieldTypeCode::LongLong),
        ("SELECT MIN(v) OVER () FROM t", FieldTypeCode::LongLong),
        (
            "SELECT FIRST_VALUE(v) OVER () FROM t",
            FieldTypeCode::LongLong,
        ),
        ("SELECT LAG(v) OVER () FROM t", FieldTypeCode::LongLong),
        // A default of the SAME type merges to that type (captured
        // `BIGINT`); a WIDENING default is refused, see
        // `window_errors_and_refusals`.
        (
            "SELECT LAG(v, 1, -1) OVER () FROM t",
            FieldTypeCode::LongLong,
        ),
    ] {
        match session.run_with_columns(sql).unwrap() {
            StmtOutput::Rows { columns, .. } => {
                assert_eq!(columns[0].1.code(), code, "result type of {sql}");
            }
            other => panic!("expected rows for {sql}, got {other:?}"),
        }
    }

    // COUNT is the one framed function that is NOT NULL (an empty frame
    // counts 0 rather than yielding NULL).
    match session
        .run_with_columns("SELECT COUNT(v) OVER () FROM t")
        .unwrap()
    {
        StmtOutput::Rows { columns, .. } => {
            assert_ne!(
                columns[0].1.flags() & tidb_datatype::FieldTypeFlags::NOT_NULL,
                0
            );
        }
        other => panic!("expected rows, got {other:?}"),
    }
}

/// Every window error this slice reproduces, checked against captured
/// TiDB errors.
#[test]
fn window_errors_and_refusals() {
    let mut session = window_session();

    // Captured: "[planner:3593]You cannot use the window function
    // 'row_number' in this context.'" -- WHERE and HAVING alike.
    assert!(matches!(
        session.run("SELECT g FROM t WHERE ROW_NUMBER() OVER (ORDER BY v) > 1"),
        Err(DriverError::WindowInvalidWindowFuncUse(ref name)) if name == "row_number"
    ));
    assert!(matches!(
        session.run("SELECT g FROM t GROUP BY g HAVING RANK() OVER (ORDER BY g) > 1"),
        Err(DriverError::WindowInvalidWindowFuncUse(ref name)) if name == "rank"
    ));

    // Captured: "[planner:1210]Incorrect arguments to ntile" for a zero,
    // a negative, and a non-constant bucket count.
    for sql in [
        "SELECT NTILE(0) OVER (ORDER BY v) FROM t",
        "SELECT NTILE(-1) OVER (ORDER BY v) FROM t",
        "SELECT NTILE(v) OVER (ORDER BY v) FROM t",
    ] {
        assert!(
            matches!(session.run(sql), Err(DriverError::WrongArguments("ntile"))),
            "expected ErrWrongArguments for {sql}"
        );
    }

    // Captured: "[planner:3579]Window name 'w' is not defined."
    assert!(matches!(
        session.run("SELECT ROW_NUMBER() OVER w FROM t"),
        Err(DriverError::WindowNoSuchWindow(ref name)) if name == "w"
    ));

    // Captured: "[planner:3581]A window which depends on another cannot
    // define partitioning."
    assert!(matches!(
        session.run(
            "SELECT ROW_NUMBER() OVER (w PARTITION BY g) FROM t \
                 WINDOW w AS (PARTITION BY g)"
        ),
        Err(DriverError::WindowNoChildPartitioning)
    ));

    // Captured: "[planner:3583]Window '<unnamed window>' cannot inherit
    // 'w' since both contain an ORDER BY clause."
    assert!(matches!(
        session.run(
            "SELECT ROW_NUMBER() OVER (w ORDER BY v) FROM t \
             WINDOW w AS (PARTITION BY g ORDER BY v)"
        ),
        Err(DriverError::WindowNoRedefineOrderBy { ref window, ref base })
            if window == "<unnamed window>" && base == "w"
    ));

    // Captured with a NAMED extending window, which Go names in the same
    // message: "[planner:3583]Window 'w2' cannot inherit 'w' since both
    // contain an ORDER BY clause."
    assert!(matches!(
        session.run(
            "SELECT ROW_NUMBER() OVER w2 FROM t \
             WINDOW w AS (PARTITION BY g ORDER BY v), w2 AS (w ORDER BY g)"
        ),
        Err(DriverError::WindowNoRedefineOrderBy { ref window, ref base })
            if window == "w2" && base == "w"
    ));

    // Captured: "[planner:3582]Window 'w' has a frame definition, so
    // cannot be referenced by another window."
    assert!(matches!(
        session.run(
            "SELECT ROW_NUMBER() OVER w2 FROM t \
             WINDOW w AS (PARTITION BY g ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), \
             w2 AS (w ORDER BY v)"
        ),
        Err(DriverError::WindowNoInheritFrame(ref base)) if base == "w"
    ));

    // Captured: "[planner:3580]There is a circularity in the window
    // dependency graph."
    assert!(matches!(
        session.run("SELECT ROW_NUMBER() OVER w FROM t WINDOW w AS (w2), w2 AS (w)"),
        Err(DriverError::WindowCircularity)
    ));

    // Captured: "[planner:1235]This version of TiDB doesn't yet support
    // 'group_concat as window function'" -- Go refuses GROUP_CONCAT
    // before it looks at any argument, and DISTINCT inside any window
    // call the same way. The parser accepts `GROUP_CONCAT(...) OVER
    // (...)` exactly like Go's grammar does (any aggregate name may
    // take an `OVER` suffix); the rejection happens at plan/exec time
    // in `tidb_exec::window::build_call`, not at parse time.
    assert!(matches!(
        session.run("SELECT GROUP_CONCAT(v) OVER (ORDER BY v) FROM t"),
        Err(DriverError::NotSupportedYet(
            "group_concat as window function"
        ))
    ));
    assert!(matches!(
        session.run("SELECT COUNT(DISTINCT v) OVER (PARTITION BY g) FROM t"),
        Err(DriverError::NotSupportedYet(
            "<window function>(DISTINCT ..)"
        ))
    ));

    // The four aggregates Go allows OVER that this build used to refuse
    // now compute here too; `json_and_approximate_aggregates` covers
    // their frame semantics. Only the SHAPE check remains: a window call
    // still needs at least one argument.
    assert!(session
        .run("SELECT g, APPROX_COUNT_DISTINCT(v) OVER (ORDER BY v) FROM t")
        .is_ok());

    // Frame validation is the PLANNER's, so it fires for every function
    // -- including the ranking ones, whose frame is then ignored.
    // Captured: "[planner:3586]Window '<unnamed window>': frame start or
    // end is negative, NULL or of non-integral type".
    for sql in [
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t",
            "SELECT ROW_NUMBER() OVER (PARTITION BY g ORDER BY v ROWS BETWEEN CURRENT ROW AND 1 PRECEDING) FROM t",
            "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM t",
        ] {
            assert!(
                matches!(session.run(sql), Err(DriverError::WindowFrameIllegal)),
                "expected 3586 for {sql}"
            );
        }

    // Captured: "[planner:3587]Window '<unnamed window>' with RANGE N
    // PRECEDING/FOLLOWING frame requires exactly one ORDER BY expression,
    // of numeric or temporal type" -- and it OUTRANKS the RANGE-offset
    // deferral above, because Go checks the ORDER BY shape first.
    assert!(matches!(
        session.run(
            "SELECT SUM(v) OVER (PARTITION BY g RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t"
        ),
        Err(DriverError::WindowRangeFrameOrderType)
    ));

    // Captured: "[planner:3588]Window '<unnamed window>' with RANGE frame
    // has ORDER BY expression of datetime type. Only INTERVAL bound value
    // allowed." -- a numeric bound over a temporal key.
    session
        .run("CREATE TABLE rt (d DATE, v BIGINT)")
        .expect("create rt");
    session
        .run("INSERT INTO rt VALUES ('2020-01-01',1),('2020-01-02',2)")
        .expect("insert rt");
    assert!(matches!(
        session.run(
            "SELECT SUM(v) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM rt"
        ),
        Err(DriverError::WindowRangeFrameTemporalType)
    ));

    // Captured: "[planner:3589]... of numeric type, INTERVAL bound value
    // not allowed." -- and, over a STRING key, 3587 wins over BOTH the
    // interval check and the interval refusal below.
    assert!(matches!(
            session.run(
                "SELECT SUM(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM t"
            ),
            Err(DriverError::WindowRangeFrameNumericType)
        ));
    session
        .run("CREATE TABLE rs (k VARCHAR(10), v BIGINT)")
        .expect("create rs");
    for sql in [
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM rs",
            "SELECT SUM(v) OVER (ORDER BY k RANGE BETWEEN INTERVAL 1 DAY PRECEDING AND CURRENT ROW) FROM rs",
        ] {
            assert!(
                matches!(session.run(sql), Err(DriverError::WindowRangeFrameOrderType)),
                "expected 3587 for {sql}"
            );
        }

    // Captured: "[planner:1210]Incorrect arguments to nth_value" -- the
    // position must be a POSITIVE integer constant, like NTILE's count.
    assert!(matches!(
        session.run("SELECT NTH_VALUE(v, 0) OVER (PARTITION BY g ORDER BY v) FROM t"),
        Err(DriverError::WrongArguments("nth_value"))
    ));
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
    // Rows come out in source order `1,3,3,7,8`.
    assert_eq!(
            row_text(session.run(
                "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) s, \
                 COUNT(*) OVER (ORDER BY k DESC RANGE BETWEEN 2 PRECEDING AND CURRENT ROW) c FROM ri"
            )),
            [["60", "3"], ["50", "2"], ["50", "2"], ["90", "2"], ["50", "1"]]
        );
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY k DESC RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) \
                 FROM ri"
        )),
        [["10"], ["60"], ["60"], ["40"], ["90"]]
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
        [["3"], ["3"], ["30"], ["30"], ["50"]]
    );
}

/// A `LAG`/`LEAD` default that WIDENS the result type: Go merges the two
/// argument types and reads BOTH operands through the merged one, so the
/// VALUE argument changes domain too.
#[test]
fn window_lag_lead_widening_default() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE ll (id BIGINT, v BIGINT, d DECIMAL(10,2), s VARCHAR(10))")
        .unwrap();
    session
        .run("INSERT INTO ll VALUES (1,10,1.50,'a'),(2,20,2.50,'b'),(3,30,3.50,'c')")
        .unwrap();

    // Captured: the integers come back as STRINGS, not just the default.
    assert_eq!(
        row_text(session.run("SELECT LAG(v,1,'zz') OVER (ORDER BY id) FROM ll")),
        [["zz"], ["10"], ["20"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LEAD(v,1,'zz') OVER (ORDER BY id) FROM ll")),
        [["20"], ["30"], ["zz"]]
    );
    // A DECIMAL default widens an integer argument to DECIMAL, and the
    // argument keeps its own scale (`10`, not the scale-padded `10.0`):
    // Go reads it through the merged type's EVAL kind, not through a
    // width-and-scale-applying conversion.
    assert_eq!(
        row_text(session.run("SELECT LAG(v,1,1.5) OVER (ORDER BY id) FROM ll")),
        [["1.5"], ["10"], ["20"]]
    );
    // The widening runs the other way too: an integer default over a
    // string argument merges to VARCHAR.
    assert_eq!(
        row_text(session.run("SELECT LAG(s,1,0) OVER (ORDER BY id) FROM ll")),
        [["0"], ["a"], ["b"]]
    );
    assert_eq!(
        row_text(session.run("SELECT LAG(d,1,'zz') OVER (ORDER BY id) FROM ll")),
        [["zz"], ["1.50"], ["2.50"]]
    );
    // Every position out of range takes the default.
    assert_eq!(
        row_text(session.run("SELECT LAG(v,5,'zz') OVER (ORDER BY id) FROM ll")),
        [["zz"], ["zz"], ["zz"]]
    );
    // A NULL default does NOT widen: Go's `InferType4ControlFuncs` drops
    // NULL-typed operands, so the result stays the argument's own type.
    assert_eq!(
        row_text(session.run("SELECT LAG(v,1,NULL) OVER (ORDER BY id) FROM ll")),
        [["NULL"], ["10"], ["20"]]
    );

    // The merged result TYPE, captured: VARCHAR for a string default,
    // DECIMAL for a decimal one, and the argument's own BIGINT when the
    // default is NULL or already an integer.
    use tidb_datatype::FieldTypeCode;
    for (sql, code) in [
        (
            "SELECT LAG(v,1,'zz') OVER (ORDER BY id) FROM ll",
            FieldTypeCode::Varchar,
        ),
        (
            "SELECT LAG(v,1,1.5) OVER (ORDER BY id) FROM ll",
            FieldTypeCode::NewDecimal,
        ),
        (
            "SELECT LAG(s,1,0) OVER (ORDER BY id) FROM ll",
            FieldTypeCode::Varchar,
        ),
        (
            "SELECT LAG(v,1,NULL) OVER (ORDER BY id) FROM ll",
            FieldTypeCode::LongLong,
        ),
        (
            "SELECT LAG(v,1,-1) OVER (ORDER BY id) FROM ll",
            FieldTypeCode::LongLong,
        ),
    ] {
        match session.run_with_columns(sql).unwrap() {
            StmtOutput::Rows { columns, .. } => {
                assert_eq!(columns[0].1.code(), code, "result type for {sql}");
            }
            other => panic!("expected rows, got {other:?}"),
        }
    }
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

/// A window function combined with `GROUP BY`: the window computes over
/// the POST-aggregation rows, so its `ORDER BY` may name an aggregate and
/// `HAVING` has already removed the groups it never sees.
#[test]
fn window_over_group_by() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE gw (g BIGINT, h BIGINT, v BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO gw VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,5),(3,1,15)")
        .unwrap();

    // Captured: the ranks follow the GROUP sums (15, 30, 35), not any
    // source row.
    assert_eq!(
        row_text(session.run(
            "SELECT g, SUM(v) s, RANK() OVER (ORDER BY SUM(v)) r FROM gw GROUP BY g \
                 ORDER BY g"
        )),
        [["1", "30", "2"], ["2", "35", "3"], ["3", "15", "1"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT g, ROW_NUMBER() OVER (ORDER BY SUM(v) DESC) r FROM gw GROUP BY g \
                 ORDER BY g"
        )),
        [["1", "2"], ["2", "1"], ["3", "3"]]
    );
    // An aggregate INSIDE a window aggregate: the running total of the
    // group sums.
    assert_eq!(
        row_text(
            session.run("SELECT g, SUM(SUM(v)) OVER (ORDER BY g) t FROM gw GROUP BY g ORDER BY g")
        ),
        [["1", "30"], ["2", "65"], ["3", "80"]]
    );
    // HAVING runs BELOW the window, so the removed group never counts:
    // ranks are 1 and 2 over the two surviving groups.
    assert_eq!(
        row_text(session.run(
            "SELECT g, SUM(v) s, RANK() OVER (ORDER BY SUM(v)) r FROM gw GROUP BY g \
                 HAVING SUM(v) > 15 ORDER BY g"
        )),
        [["1", "30", "1"], ["2", "35", "2"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT g, SUM(v) s, RANK() OVER (ORDER BY g) r FROM gw GROUP BY g \
                 HAVING SUM(v) > 15 ORDER BY g"
        )),
        [["1", "30", "1"], ["2", "35", "2"]]
    );
    // A window PARTITION BY over an aggregate.
    assert_eq!(
        row_text(session.run(
            "SELECT g, COUNT(*) c, RANK() OVER (PARTITION BY COUNT(*) ORDER BY g) r \
                 FROM gw GROUP BY g ORDER BY g"
        )),
        [["1", "2", "1"], ["2", "2", "2"], ["3", "1", "1"]]
    );
    // LAG and PERCENT_RANK over the grouped rows.
    assert_eq!(
        row_text(
            session.run("SELECT g, LAG(SUM(v)) OVER (ORDER BY g) l FROM gw GROUP BY g ORDER BY g")
        ),
        [["1", "NULL"], ["2", "30"], ["3", "35"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT g, PERCENT_RANK() OVER (ORDER BY SUM(v)) p FROM gw GROUP BY g ORDER BY g"
        )),
        [["1", "0.5"], ["2", "1"], ["3", "0"]]
    );
    // A window over an implicit single-group aggregation (no GROUP BY).
    assert_eq!(
        row_text(session.run("SELECT MAX(v) m, RANK() OVER (ORDER BY MAX(v)) r FROM gw")),
        [["30", "1"]]
    );
    // The outer ORDER BY sorts the already-computed window value, through
    // its select alias.
    assert_eq!(
        row_text(session.run(
            "SELECT g, SUM(v) s, RANK() OVER (ORDER BY SUM(v)) r FROM gw GROUP BY g \
                 ORDER BY r DESC"
        )),
        [["2", "35", "3"], ["1", "30", "2"], ["3", "15", "1"]]
    );
    // A window over a GROUPED column needs no aggregate at all.
    assert_eq!(
        row_text(session.run("SELECT g, RANK() OVER (ORDER BY g) r FROM gw GROUP BY g ORDER BY g")),
        [["1", "1"], ["2", "2"], ["3", "3"]]
    );
    // A window in HAVING is still Go's 3593, wherever the query groups.
    assert!(matches!(
        session.run("SELECT g FROM gw GROUP BY g HAVING RANK() OVER (ORDER BY g) = 1"),
        Err(DriverError::WindowInvalidWindowFuncUse(ref name)) if name == "rank"
    ));
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

/// `FIRST_VALUE`/`LAST_VALUE`/`NTH_VALUE`, which DO read the frame -- so
/// `LAST_VALUE` under the default frame famously returns the current PEER
/// GROUP's last row, not the partition's.
#[test]
fn window_value_functions_read_the_frame() {
    let mut session = window_session();

    assert_eq!(
        row_text(session.run(
            "SELECT FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY v) AS f FROM t ORDER BY g, v"
        )),
        [["10"], ["10"], ["10"], ["10"], ["10"], ["5"], ["5"], ["5"]]
    );

    // The default frame ends at the current PEER GROUP, so LAST_VALUE is
    // the row's own peer-group maximum -- 40 appears only on the last row.
    assert_eq!(
        row_text(session.run(
            "SELECT LAST_VALUE(v) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
        )),
        [["10"], ["20"], ["20"], ["30"], ["40"], ["5"], ["5"], ["7"]]
    );
    // Spelling out the whole partition is what returns the partition's
    // last row on EVERY row.
    assert_eq!(
        row_text(session.run(
            "SELECT LAST_VALUE(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN UNBOUNDED \
                 PRECEDING AND UNBOUNDED FOLLOWING) AS l FROM t ORDER BY g, v"
        )),
        [["40"], ["40"], ["40"], ["40"], ["40"], ["7"], ["7"], ["7"]]
    );

    // NTH_VALUE is NULL while the frame holds fewer than n rows.
    assert_eq!(
        row_text(session.run(
            "SELECT NTH_VALUE(v, 3) OVER (PARTITION BY g ORDER BY v) AS n FROM t ORDER BY g, v"
        )),
        [
            ["NULL"],
            ["20"],
            ["20"],
            ["20"],
            ["20"],
            ["NULL"],
            ["NULL"],
            ["7"],
        ]
    );
    // Counted from the FRAME's start, not the partition's.
    assert_eq!(
        row_text(session.run(
            "SELECT NTH_VALUE(v, 2) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 1 PRECEDING \
                 AND CURRENT ROW) AS n FROM t ORDER BY g, v"
        )),
        [
            ["NULL"],
            ["20"],
            ["20"],
            ["30"],
            ["40"],
            ["NULL"],
            ["5"],
            ["7"],
        ]
    );
}

/// `LAG`/`LEAD`, which address the sorted partition directly and IGNORE
/// the frame entirely.
#[test]
fn window_lag_and_lead() {
    let mut session = window_session();

    // The default offset is 1, and the partition's first row is NULL.
    assert_eq!(
        row_text(
            session.run("SELECT LAG(v) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v")
        ),
        [
            ["NULL"],
            ["10"],
            ["20"],
            ["20"],
            ["30"],
            ["NULL"],
            ["5"],
            ["5"],
        ]
    );
    assert_eq!(
        row_text(
            session
                .run("SELECT LAG(v, 2) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v")
        ),
        [
            ["NULL"],
            ["NULL"],
            ["10"],
            ["20"],
            ["20"],
            ["NULL"],
            ["NULL"],
            ["5"],
        ]
    );
    // The third argument fills EVERY out-of-range position.
    assert_eq!(
        row_text(session.run(
            "SELECT LAG(v, 2, -1) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
        )),
        [
            ["-1"],
            ["-1"],
            ["10"],
            ["20"],
            ["20"],
            ["-1"],
            ["-1"],
            ["5"]
        ]
    );
    // Offset 0 is the current row.
    assert_eq!(
        row_text(
            session
                .run("SELECT LAG(v, 0) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v")
        ),
        [["10"], ["20"], ["20"], ["30"], ["40"], ["5"], ["5"], ["7"]]
    );

    // LEAD runs off the partition's END instead.
    assert_eq!(
        row_text(
            session
                .run("SELECT LEAD(v) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v")
        ),
        [
            ["20"],
            ["20"],
            ["30"],
            ["40"],
            ["NULL"],
            ["5"],
            ["7"],
            ["NULL"],
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT LEAD(v, 2, -7) OVER (PARTITION BY g ORDER BY v) AS l FROM t ORDER BY g, v"
        )),
        [
            ["20"],
            ["30"],
            ["40"],
            ["-7"],
            ["-7"],
            ["7"],
            ["-7"],
            ["-7"]
        ]
    );

    // A frame is written but IGNORED: the result is identical to the
    // frame-less LAG above (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT LAG(v) OVER (PARTITION BY g ORDER BY v ROWS BETWEEN 2 PRECEDING AND \
                 1 PRECEDING) AS l FROM t ORDER BY g, v"
        )),
        [
            ["NULL"],
            ["10"],
            ["20"],
            ["20"],
            ["30"],
            ["NULL"],
            ["5"],
            ["5"],
        ]
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

/// The pipeline ABOVE the window stage -- an `ORDER BY`-only window,
/// `DISTINCT`, and `LIMIT` -- checked against captured TiDB output.
#[test]
fn window_feeds_the_ordinary_pipeline() {
    let mut session = window_session();

    // The window is never projected, only sorted by: `v` descending
    // through its ROW_NUMBER, so the two `g = 2` rows with the smallest
    // `v` come last (captured).
    assert_eq!(
        row_text(session.run("SELECT g FROM t ORDER BY ROW_NUMBER() OVER (ORDER BY v) DESC")),
        [["1"], ["1"], ["1"], ["1"], ["1"], ["2"], ["2"], ["2"]]
    );

    // DISTINCT deduplicates the already-computed window column
    // (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT DISTINCT g, NTILE(2) OVER (PARTITION BY g ORDER BY v) \
                 FROM t ORDER BY 1, 2"
        )),
        [["1", "1"], ["1", "2"], ["2", "1"], ["2", "2"]]
    );

    // LIMIT applies after the outer ORDER BY over the window column
    // (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) rn \
                 FROM t ORDER BY rn DESC, g LIMIT 3"
        )),
        [["1", "40", "5"], ["1", "30", "4"], ["1", "20", "3"]]
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

/// A window call nested inside a LARGER select expression, which Go
/// evaluates in the projection ABOVE the window operator -- over a plain
/// query and over a grouped one alike.
///
/// Every expectation is captured TiDB output over `(1,10),(1,20),(1,20),
/// (2,30),(2,40)`.
#[test]
fn window_nested_in_larger_expression() {
    let mut session = Session::new();
    session.run("CREATE TABLE tw (g BIGINT, v BIGINT)").unwrap();
    session
        .run("INSERT INTO tw VALUES (1,10),(1,20),(1,20),(2,30),(2,40)")
        .unwrap();

    // Arithmetic around a ranking function, and a string function over
    // one.
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, RANK() OVER (PARTITION BY g ORDER BY v) + 1 FROM tw \
                 ORDER BY g, v"
        )),
        [
            ["1", "10", "2"],
            ["1", "20", "3"],
            ["1", "20", "3"],
            ["2", "30", "2"],
            ["2", "40", "3"]
        ]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT CONCAT('#', ROW_NUMBER() OVER (PARTITION BY g ORDER BY v)) FROM tw \
                 ORDER BY g, v"
        )),
        [["#1"], ["#2"], ["#3"], ["#1"], ["#2"]]
    );

    // TWO window calls in one expression, both over the same named
    // window (captured `3,6,7,3,6`).
    assert_eq!(
        row_text(session.run(
            "SELECT RANK() OVER w * 2 + ROW_NUMBER() OVER w FROM tw \
                 WINDOW w AS (PARTITION BY g ORDER BY v) ORDER BY g, v"
        )),
        [["3"], ["6"], ["7"], ["3"], ["6"]]
    );

    // A window value inside a control function, and one under unary
    // minus (captured `-1,-2,-3,-1,-2`).
    assert_eq!(
        row_text(session.run(
            "SELECT IF(ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) = 1, 'first', 'rest') \
                 FROM tw ORDER BY g, v"
        )),
        [["first"], ["rest"], ["rest"], ["first"], ["rest"]]
    );
    assert_eq!(
        row_text(
            session
                .run("SELECT -ROW_NUMBER() OVER (PARTITION BY g ORDER BY v) FROM tw ORDER BY g, v")
        ),
        [["-1"], ["-2"], ["-3"], ["-1"], ["-2"]]
    );

    // Two window calls divided by each other (captured 16.6667 / 35.0000
    // -- the division carries div_precision_increment's scale).
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (PARTITION BY g) / COUNT(*) OVER (PARTITION BY g) \
                 FROM tw ORDER BY g, v"
        )),
        [
            ["16.6667"],
            ["16.6667"],
            ["16.6667"],
            ["35.0000"],
            ["35.0000"]
        ]
    );

    // Over a GROUPED query: the window computes over the aggregation's
    // output rows, and the larger expression over THAT.
    assert_eq!(
        row_text(session.run(
            "SELECT g, SUM(v), RANK() OVER (ORDER BY SUM(v)) + 100 FROM tw \
                 GROUP BY g ORDER BY g"
        )),
        [["1", "50", "101"], ["2", "70", "102"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT g, CONCAT('g', RANK() OVER (ORDER BY SUM(v) DESC)) FROM tw \
                 GROUP BY g ORDER BY g"
        )),
        [["1", "g2"], ["2", "g1"]]
    );
    // An aggregate OUTSIDE the window call, added to the window's value
    // (captured `51` / `72`).
    assert_eq!(
        row_text(session.run(
            "SELECT g, SUM(v) + ROW_NUMBER() OVER (ORDER BY g) FROM tw GROUP BY g ORDER BY g"
        )),
        [["1", "51"], ["2", "72"]]
    );

    // The outer ORDER BY sorts the ALIASED nested expression (captured
    // `3,3,3,2,2`).
    assert_eq!(
        row_text(session.run(
            "SELECT g, v, RANK() OVER (PARTITION BY g ORDER BY v) + 1 AS r FROM tw \
                 ORDER BY r DESC, g, v"
        )),
        [
            ["1", "20", "3"],
            ["1", "20", "3"],
            ["2", "40", "3"],
            ["1", "10", "2"],
            ["2", "30", "2"]
        ]
    );
}

/// A window function over `GROUP BY ... WITH ROLLUP`: the window sees the
/// rollup OUTPUT rows, supergroup rows included, and their NULLed columns
/// participate in `PARTITION BY`/`ORDER BY` like any other NULL.
///
/// Every expectation is captured TiDB output over
/// `(1,1,10),(1,2,20),(2,1,30),(2,2,40)`.
#[test]
fn window_over_rollup() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE tr (a BIGINT, b BIGINT, v BIGINT)")
        .unwrap();
    session
        .run("INSERT INTO tr VALUES (1,1,10),(1,2,20),(2,1,30),(2,2,40)")
        .unwrap();

    // Seven output rows -- four groups, two subtotals, one grand total --
    // numbered in the window's own ORDER BY. The outer ORDER BY is
    // written because a rollup's own row order is nondeterministic in Go.
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, SUM(v), ROW_NUMBER() OVER (ORDER BY a, b) FROM tr \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
        )),
        [
            ["NULL", "NULL", "100", "1"],
            ["1", "NULL", "30", "2"],
            ["1", "1", "10", "3"],
            ["1", "2", "20", "4"],
            ["2", "NULL", "70", "5"],
            ["2", "1", "30", "6"],
            ["2", "2", "40", "7"]
        ]
    );

    // PARTITION BY a puts each subtotal row in ITS OWN group's partition
    // (its `b` is NULL, which sorts first), and the grand total alone in
    // the NULL partition.
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, RANK() OVER (PARTITION BY a ORDER BY b) FROM tr \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
        )),
        [
            ["NULL", "NULL", "1"],
            ["1", "NULL", "1"],
            ["1", "1", "2"],
            ["1", "2", "3"],
            ["2", "NULL", "1"],
            ["2", "1", "2"],
            ["2", "2", "3"]
        ]
    );

    // An aggregate INSIDE the window call sums the rollup rows of the
    // partition -- the subtotal row included, which is why `a = 1` totals
    // 60 rather than 30 (captured).
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, SUM(SUM(v)) OVER (PARTITION BY a) FROM tr \
                 GROUP BY a, b WITH ROLLUP ORDER BY a, b"
        )),
        [
            ["NULL", "NULL", "100"],
            ["1", "NULL", "60"],
            ["1", "1", "60"],
            ["1", "2", "60"],
            ["2", "NULL", "140"],
            ["2", "1", "140"],
            ["2", "2", "140"]
        ]
    );

    // GROUPING() tells a rollup NULL from a data NULL, and a window may
    // partition by it (captured: the grand total alone has grouping(a) =
    // 1, so it is row 1 of its own partition).
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, GROUPING(a), \
                 ROW_NUMBER() OVER (PARTITION BY GROUPING(a) ORDER BY a, b) \
                 FROM tr GROUP BY a, b WITH ROLLUP ORDER BY a, b"
        )),
        [
            ["NULL", "NULL", "1", "1"],
            ["1", "NULL", "0", "1"],
            ["1", "1", "0", "2"],
            ["1", "2", "0", "3"],
            ["2", "NULL", "0", "4"],
            ["2", "1", "0", "5"],
            ["2", "2", "0", "6"]
        ]
    );

    // RANK over the rollup's SUMs: the `a = 1` subtotal (30) ties with
    // the `(2,1)` group (30), so both are rank 4 and the next jumps to 6.
    assert_eq!(
        row_text(session.run(
            "SELECT a, b, SUM(v), RANK() OVER (ORDER BY SUM(v) DESC) FROM tr \
                 GROUP BY a, b WITH ROLLUP ORDER BY SUM(v) DESC, a, b"
        )),
        [
            ["NULL", "NULL", "100", "1"],
            ["2", "NULL", "70", "2"],
            ["2", "2", "40", "3"],
            ["1", "NULL", "30", "4"],
            ["2", "1", "30", "4"],
            ["1", "2", "20", "6"],
            ["1", "1", "10", "7"]
        ]
    );
}

/// The bitwise and variance/stddev aggregates AS window functions, which
/// Go allows over any frame.
///
/// Every expectation is captured TiDB output over
/// `(1,3),(1,5),(1,6),(2,1)`.
#[test]
fn window_bit_and_variance_aggregates() {
    let mut session = Session::new();
    session.run("CREATE TABLE ta (g BIGINT, v BIGINT)").unwrap();
    session
        .run("INSERT INTO ta VALUES (1,3),(1,5),(1,6),(2,1)")
        .unwrap();

    // The default frame is the running peer-inclusive one, so each row
    // folds every value up to and including itself.
    assert_eq!(
        row_text(
            session.run("SELECT BIT_AND(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v")
        ),
        [["3"], ["1"], ["0"], ["1"]]
    );
    assert_eq!(
        row_text(
            session.run("SELECT BIT_OR(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v")
        ),
        [["3"], ["7"], ["7"], ["1"]]
    );
    assert_eq!(
        row_text(
            session.run("SELECT BIT_XOR(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v")
        ),
        [["3"], ["6"], ["0"], ["1"]]
    );

    // POPULATION forms divide by the frame's row count (a single row is
    // 0, not NULL); SAMPLE forms divide by count - 1 and are NULL for a
    // single row. `STDDEV`/`STD`/`VARIANCE` are the population forms.
    for name in ["VAR_POP", "VARIANCE"] {
        assert_eq!(
            row_text(session.run(&format!(
                "SELECT {name}(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v"
            ))),
            [["0"], ["1"], ["1.5555555555555554"], ["0"]],
            "for {name}"
        );
    }
    for name in ["STDDEV_POP", "STDDEV", "STD"] {
        assert_eq!(
            row_text(session.run(&format!(
                "SELECT {name}(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v"
            ))),
            [["0"], ["1"], ["1.247219128924647"], ["0"]],
            "for {name}"
        );
    }
    assert_eq!(
        row_text(
            session
                .run("SELECT VAR_SAMP(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v")
        ),
        [["NULL"], ["2"], ["2.333333333333333"], ["NULL"]]
    );
    assert_eq!(
        row_text(
            session.run(
                "SELECT STDDEV_SAMP(v) OVER (PARTITION BY g ORDER BY v) FROM ta ORDER BY g, v"
            )
        ),
        [
            ["NULL"],
            ["1.4142135623730951"],
            ["1.5275252316519465"],
            ["NULL"]
        ]
    );

    // With no window ORDER BY the frame is the whole partition, and an
    // explicit ROWS frame narrows it the same way it does for SUM.
    assert_eq!(
        row_text(session.run("SELECT BIT_AND(v) OVER (PARTITION BY g) FROM ta ORDER BY g, v")),
        [["0"], ["0"], ["0"], ["1"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT STDDEV_POP(v) OVER (PARTITION BY g \
                 ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM ta ORDER BY g, v"
        )),
        [["0"], ["1"], ["0.5"], ["0"]]
    );
    // An EMPTY frame folds to the bit operator's IDENTITY (0 for XOR)
    // but is NULL for the sample variance -- captured.
    assert_eq!(
        row_text(session.run(
            "SELECT BIT_XOR(v) OVER (PARTITION BY g \
                 ROWS BETWEEN 2 FOLLOWING AND 3 FOLLOWING) FROM ta ORDER BY g, v"
        )),
        [["6"], ["0"], ["0"], ["0"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT VAR_SAMP(v) OVER (PARTITION BY g \
                 ROWS BETWEEN CURRENT ROW AND CURRENT ROW) FROM ta ORDER BY g, v"
        )),
        [["NULL"], ["NULL"], ["NULL"], ["NULL"]]
    );

    // An all-NULL frame: the variance family is NULL, BIT_AND folds to
    // its all-ones identity -- which the SIGNED result column prints as
    // `-1` (captured) -- and BIT_OR/BIT_XOR to 0.
    session.run("CREATE TABLE tn (g BIGINT, v BIGINT)").unwrap();
    session
        .run("INSERT INTO tn VALUES (1,NULL),(1,4),(1,NULL)")
        .unwrap();
    assert_eq!(
        row_text(
            session.run("SELECT VAR_POP(v) OVER (PARTITION BY g ORDER BY v) FROM tn ORDER BY g, v")
        ),
        [["NULL"], ["NULL"], ["0"]]
    );
    assert_eq!(
        row_text(
            session.run("SELECT BIT_AND(v) OVER (PARTITION BY g ORDER BY v) FROM tn ORDER BY g, v")
        ),
        [["-1"], ["-1"], ["4"]]
    );
    assert_eq!(
        row_text(
            session.run("SELECT BIT_OR(v) OVER (PARTITION BY g ORDER BY v) FROM tn ORDER BY g, v")
        ),
        [["0"], ["0"], ["4"]]
    );
}

/// A named window that EXTENDS another, including a chain of three and a
/// forward reference -- Go resolves the `WINDOW` clause as a graph, not
/// in written order.
///
/// Every expectation is captured TiDB output.
#[test]
fn window_base_window_references() {
    let mut session = Session::new();
    session.run("CREATE TABLE tw (g BIGINT, v BIGINT)").unwrap();
    session
        .run("INSERT INTO tw VALUES (1,10),(1,20),(1,20),(2,30),(2,40)")
        .unwrap();

    // `w2 AS (w ORDER BY v)` inherits w's PARTITION BY and adds the
    // order; a chain of three and a FORWARD reference resolve the same.
    for sql in [
        "SELECT ROW_NUMBER() OVER w2 FROM tw \
             WINDOW w AS (PARTITION BY g), w2 AS (w ORDER BY v) ORDER BY g, v",
        "SELECT ROW_NUMBER() OVER w2 FROM tw \
             WINDOW w2 AS (w ORDER BY v), w AS (PARTITION BY g) ORDER BY g, v",
        "SELECT ROW_NUMBER() OVER w3 FROM tw \
             WINDOW w AS (PARTITION BY g), w2 AS (w ORDER BY v), w3 AS (w2) ORDER BY g, v",
    ] {
        assert_eq!(
            row_text(session.run(sql)),
            [["1"], ["2"], ["3"], ["1"], ["2"]],
            "for {sql}"
        );
    }

    // A bare `w2 AS (w)` inherits everything, and an extension may add
    // its OWN frame over an inherited order.
    assert_eq!(
        row_text(session.run(
            "SELECT ROW_NUMBER() OVER w2 FROM tw \
                 WINDOW w AS (PARTITION BY g ORDER BY v), w2 AS (w) ORDER BY g, v"
        )),
        [["1"], ["2"], ["3"], ["1"], ["2"]]
    );
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER w2 FROM tw \
                 WINDOW w AS (PARTITION BY g ORDER BY v), \
                 w2 AS (w ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) ORDER BY g, v"
        )),
        [["10"], ["30"], ["50"], ["30"], ["70"]]
    );
}
