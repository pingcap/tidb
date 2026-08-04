//! Two rules a window function gets WRONG rather than refuses: the peer /
//! partition identity of a `_ci` key, and the frame shapes Go's
//! `checkOriginWindowSpec` refuses outright.
//!
//! Every expectation here is captured from a real TiDB session (mockstore,
//! `pkg/session`), including the error CODES.

use crate::tests_support::*;
use crate::*;

/// A `_ci` fixture where the case-folded groups (`a/A/a`, `b/B`) differ from
/// the byte-wise ones (`A`, `B`, `a`, `a`, `b`), so a collation-blind
/// comparison is visible in every column.
fn ci_session() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE w1 (id INT, s VARCHAR(10) COLLATE utf8mb4_general_ci, v INT)")
        .unwrap();
    session
        .run("INSERT INTO w1 VALUES (1,'a',10),(2,'A',20),(3,'b',30),(4,'B',40),(5,'a',50)")
        .unwrap();
    session
}

/// The window `ORDER BY` key's own collation decides who is a PEER, which is
/// what `RANK`/`DENSE_RANK` skip over and what the default frame's
/// `CURRENT ROW` includes.
///
/// Captured TiDB output. Under `utf8mb4_general_ci` the three `a/A/a` rows are
/// one peer group and the two `b/B` rows another, so `RANK` is `1,1,4,4,1` --
/// a byte-wise comparison instead gives five singleton groups and `3,1,5,2,3`.
#[test]
fn window_peers_follow_the_order_key_collation() {
    let mut session = ci_session();

    assert_eq!(
        row_text(session.run(
            "SELECT id, s, RANK() OVER (ORDER BY s) r, DENSE_RANK() OVER (ORDER BY s) dr \
                 FROM w1 ORDER BY id"
        )),
        [
            ["1", "a", "1", "1"],
            ["2", "A", "1", "1"],
            ["3", "b", "4", "2"],
            ["4", "B", "4", "2"],
            ["5", "a", "1", "1"],
        ]
    );

    // The default frame is `RANGE UNBOUNDED PRECEDING AND CURRENT ROW`, whose
    // CURRENT ROW is peer-inclusive: every `a/A/a` row shows the whole group's
    // running total 80, and every `b/B` row shows 150.
    assert_eq!(
        row_text(session.run("SELECT id, SUM(v) OVER (ORDER BY s) rs FROM w1 ORDER BY id")),
        [
            ["1", "80"],
            ["2", "80"],
            ["3", "150"],
            ["4", "150"],
            ["5", "80"]
        ]
    );

    // The sort ITSELF is collation-ordered too, so `a,A,a` sort before `b,B`
    // and the tie-break on `id` runs inside the case-folded group.
    assert_eq!(
        row_text(
            session.run("SELECT id, ROW_NUMBER() OVER (ORDER BY s, id) rn FROM w1 ORDER BY id")
        ),
        [["1", "1"], ["2", "2"], ["3", "4"], ["4", "5"], ["5", "3"]]
    );
}

/// `PARTITION BY` on a `_ci` key IDENTIFIES the partitions under that
/// collation, so `a/A/a` is ONE partition of three rows summing 80 -- not the
/// two byte-wise partitions of 60 and 20.
#[test]
fn window_partitions_follow_the_partition_key_collation() {
    let mut session = ci_session();

    assert_eq!(
        row_text(session.run(
            "SELECT id, SUM(v) OVER (PARTITION BY s) sm, COUNT(*) OVER (PARTITION BY s) c \
                 FROM w1 ORDER BY id"
        )),
        [
            ["1", "80", "3"],
            ["2", "80", "3"],
            ["3", "70", "2"],
            ["4", "70", "2"],
            ["5", "80", "3"],
        ]
    );
}

/// Go's `checkOriginWindowSpec` refuses two frame shapes a `start`-versus-
/// `end` rank test cannot see, because both bounds have the SAME rank:
/// `UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING` and `UNBOUNDED PRECEDING AND
/// UNBOUNDED PRECEDING`. Both used to be accepted and to return NULL for every
/// row -- a wrong answer, silently.
///
/// The rules also run BEFORE any offset is folded, so a frame that is illegal
/// in shape AND carries a bad offset reports the SHAPE error.
///
/// Every code below is captured TiDB output.
#[test]
fn window_frame_shape_is_refused_with_gos_own_code() {
    let mut session = ci_session();

    // "[planner:3584]Window '<unnamed window>': frame start cannot be
    // UNBOUNDED FOLLOWING."
    for sql in [
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) \
             FROM w1",
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM w1",
        "SELECT SUM(v) OVER (ORDER BY id ROWS UNBOUNDED FOLLOWING) FROM w1",
        // The shape rule outranks the malformed 1.5 offset (alone, 3586).
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED FOLLOWING AND 1.5 FOLLOWING) \
             FROM w1",
        // ... and it applies to RANGE and to a window with no ORDER BY at all.
        "SELECT SUM(v) OVER (ORDER BY s RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) \
             FROM w1",
        "SELECT SUM(v) OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) FROM w1",
    ] {
        assert!(
            matches!(session.run(sql), Err(DriverError::WindowFrameStartIllegal)),
            "expected 3584 for {sql}"
        );
    }

    // "[planner:3585]Window '<unnamed window>': frame end cannot be
    // UNBOUNDED PRECEDING."
    for sql in [
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED PRECEDING) \
             FROM w1",
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) FROM w1",
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM w1",
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1 FOLLOWING AND UNBOUNDED PRECEDING) FROM w1",
        "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1.5 FOLLOWING AND UNBOUNDED PRECEDING) \
             FROM w1",
        // The end rule outranks 3587's ORDER BY shape check, too.
        "SELECT SUM(v) OVER (ORDER BY id, v RANGE BETWEEN 1 PRECEDING AND UNBOUNDED PRECEDING) \
             FROM w1",
    ] {
        assert!(
            matches!(session.run(sql), Err(DriverError::WindowFrameEndIllegal)),
            "expected 3585 for {sql}"
        );
    }

    // The rendered codes and messages, exactly as TiDB writes them.
    let start = DriverError::WindowFrameStartIllegal.to_mysql_error();
    assert_eq!(start.code, 3584);
    assert_eq!(
        start.message,
        "Window '<unnamed window>': frame start cannot be UNBOUNDED FOLLOWING."
    );
    let end = DriverError::WindowFrameEndIllegal.to_mysql_error();
    assert_eq!(end.code, 3585);
    assert_eq!(
        end.message,
        "Window '<unnamed window>': frame end cannot be UNBOUNDED PRECEDING."
    );

    // The CONTROL: the frames Go still accepts, with UNBOUNDED FOLLOWING as
    // the END, keep computing. Captured over `v = 10,20,30,40,50`.
    assert_eq!(
        row_text(session.run(
            "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) \
                 FROM w1 ORDER BY id"
        )),
        [["150"], ["140"], ["120"], ["90"], ["50"]]
    );
    // And a bad OFFSET on a legal shape is still 3586, not one of the new codes.
    assert!(matches!(
        session.run(
            "SELECT SUM(v) OVER (ORDER BY id ROWS BETWEEN 1.5 PRECEDING AND CURRENT ROW) FROM w1"
        ),
        Err(DriverError::WindowFrameIllegal)
    ));
}
