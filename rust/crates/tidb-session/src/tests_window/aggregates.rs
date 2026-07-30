//! Ordinary aggregates used as window functions, including over
//! `GROUP BY` and `WITH ROLLUP` inputs.

use crate::tests_support::*;
use crate::*;

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
