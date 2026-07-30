//! Value window functions: `LAG`, `LEAD`, `FIRST_VALUE`, `LAST_VALUE` and
//! `NTH_VALUE`, which read a row out of the frame.

use crate::tests_support::*;
use crate::*;

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
