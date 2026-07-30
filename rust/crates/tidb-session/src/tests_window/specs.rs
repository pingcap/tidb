//! Window specifications and how a window fits the rest of the statement:
//! named windows, `OVER` inheritance, result types, refusals, the outer
//! `ORDER BY`, and windows nested inside larger expressions.

use crate::tests_support::*;
use crate::*;

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
