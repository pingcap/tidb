#![cfg(test)]

//! Receipts for `tidb_executor::driver::correlated_agg_decorrelate::
//! rewrite_count_having_fields` — Go's grouped-below decorrelation arm for a
//! SELECT-list scalar `COUNT` subquery with a HAVING (`rule_decorrelate.go`,
//! the `havingConds` branch of the `LogicalApply -> LogicalAggregation ->
//! Selection` arm).
//!
//! The three-way value split the arm must preserve, per outer row:
//!
//!  * matching group, HAVING true  -> the count;
//!  * matching group, HAVING false -> NULL (the subquery returned no row);
//!  * NO matching group            -> the HAVING evaluated over COUNT's
//!    empty-input default 0 — so `HAVING k = 0` answers 0 there, NOT NULL.
//!
//! Distinguishing the last two is the entire reason Go removes the pulled-up
//! HAVING from the join again and evaluates it in a projection over
//! `IFNULL(count, 0)`.
//!
//! Row assertions are verified two ways: against SQL aggregate semantics
//! derived by hand, and DIFFERENTIALLY inside this engine — each statement is
//! replayed as its `COUNT(1)+0` twin, a spelling the rewrite refuses (the
//! inner field is no longer a bare aggregate), so the same question runs
//! through the untouched Apply path and must agree. The recorded plan shape
//! is pinned by `tests/integrationtest/r/explain_easy.result`'s
//! `(select count(1) k from t1 s where s.c1 = t1.c1 having k != 0)`
//! statement, replayed by the difftest ratchet.

use crate::tests_support::*;
use crate::*;

fn rows(session: &mut Session, sql: &str) -> Vec<Vec<String>> {
    row_text(session.run(sql))
}

/// One schema for every test: `t1` rows 1..3, `t2` groups of size 2 (a=1),
/// size 2 with one NULL `b` (a=2), and no group at all (a=3).
fn session_with_groups() -> Session {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t1 (c1 INT PRIMARY KEY, c2 INT)")
        .unwrap();
    session.run("CREATE TABLE t2 (a INT, b INT)").unwrap();
    session
        .run("INSERT INTO t1 VALUES (1,10),(2,20),(3,30)")
        .unwrap();
    session
        .run("INSERT INTO t2 VALUES (1,1),(1,2),(2,1),(2,NULL)")
        .unwrap();
    session
}

/// Asserts the decorrelated statement and its Apply-path `+0` twin both
/// answer `expected`. `sql` must contain `COUNT(<arg>)` exactly once so the
/// twin can be spelled mechanically.
fn assert_both_paths(session: &mut Session, sql: &str, expected: &[[&str; 2]]) {
    let expected: Vec<Vec<String>> = expected
        .iter()
        .map(|row| row.iter().map(|cell| (*cell).to_owned()).collect())
        .collect();
    assert_eq!(rows(session, sql), expected, "decorrelated: {sql}");
    let close = sql
        .find("COUNT(")
        .map(|at| sql[at..].find(')').unwrap() + at);
    let twin = format!(
        "{}+0{}",
        &sql[..close.unwrap() + 1],
        &sql[close.unwrap() + 1..]
    );
    assert_eq!(rows(session, &twin), expected, "apply twin: {twin}");
}

/// The three-way split for `COUNT(1)`: count where HAVING passes, NULL where
/// it fails, and the default-driven answer where no group exists.
#[test]
fn a_having_over_count_keeps_the_empty_group_default() {
    let mut session = session_with_groups();

    // a=1 and a=2 both count 2; a=3 has no group, so the HAVING reads the
    // default 0 and passes.
    assert_both_paths(
        &mut session,
        "SELECT c1, (SELECT COUNT(1) k FROM t2 WHERE t2.a = t1.c1 HAVING k != 1) \
         FROM t1 ORDER BY c1",
        &[["1", "2"], ["2", "2"], ["3", "0"]],
    );

    // The failing groups answer NULL — the subquery returned no row — while
    // the passing ones keep their count.
    assert_both_paths(
        &mut session,
        "SELECT c1, (SELECT COUNT(1) k FROM t2 WHERE t2.a = t1.c1 HAVING k = 2) \
         FROM t1 ORDER BY c1",
        &[["1", "2"], ["2", "2"], ["3", "NULL"]],
    );

    // The disambiguation itself: `k = 0` is FALSE for every real group and
    // TRUE only over the empty-input default, so ONLY the group-less outer
    // row answers 0. A rewrite that left the HAVING on the join would turn
    // all three into NULL-extended rows and answer 0 everywhere.
    assert_both_paths(
        &mut session,
        "SELECT c1, (SELECT COUNT(1) k FROM t2 WHERE t2.a = t1.c1 HAVING k = 0) \
         FROM t1 ORDER BY c1",
        &[["1", "NULL"], ["2", "NULL"], ["3", "0"]],
    );
}

/// `COUNT(column)` counts non-NULL values per group, and a compound HAVING
/// evaluates over the same defaulted output.
#[test]
fn a_column_count_and_a_compound_having_read_the_same_default() {
    let mut session = session_with_groups();

    // a=2's NULL `b` drops out of COUNT(b): 1, and the HAVING fails there.
    assert_both_paths(
        &mut session,
        "SELECT c1, (SELECT COUNT(b) k FROM t2 WHERE t2.a = t1.c1 HAVING k != 1) \
         FROM t1 ORDER BY c1",
        &[["1", "2"], ["2", "NULL"], ["3", "0"]],
    );

    // Both conjuncts read the defaulted count: no group satisfies 0<k<2 for
    // COUNT(1) here (counts are 2, 2, and default 0).
    assert_both_paths(
        &mut session,
        "SELECT c1, (SELECT COUNT(1) k FROM t2 WHERE t2.a = t1.c1 \
         HAVING k > 0 AND k < 2) FROM t1 ORDER BY c1",
        &[["1", "NULL"], ["2", "NULL"], ["3", "NULL"]],
    );
}

/// The HAVING may repeat the aggregate instead of naming its alias; both
/// spellings reference the same output column in Go's logical plan.
#[test]
fn a_having_may_repeat_the_aggregate_expression() {
    let mut session = session_with_groups();
    assert_eq!(
        rows(
            &mut session,
            "SELECT c1, (SELECT COUNT(1) k FROM t2 s WHERE s.a = t1.c1 \
             HAVING COUNT(1) != 0) FROM t1 ORDER BY c1",
        ),
        [["1", "2"], ["2", "2"], ["3", "NULL"]]
    );
}

/// The recorded `explain_easy` shape: the arm produces the left join Go
/// prints, and the join's sides read the TABLE in key order for the merge
/// join — not the covering index the Apply plan used to pick.
///
/// Pinned against `tests/integrationtest/r/explain_easy.result`:
///
/// ```text
/// Projection            if(ne(ifnull(Column, 0), 0), ifnull(Column, 0), <nil>)->Column
/// └─MergeJoin           left outer join, left key:t1.c1, right key:t1.c1
///   ├─Projection(Build) 1->Column, t1.c1
///   │ └─TableReader     data:TableFullScan
///   │   └─TableFullScan table:s  keep order:true
///   └─TableReader(Probe) data:TableFullScan
///     └─TableFullScan   table:t1  keep order:true
/// ```
///
/// This tier still prints a StreamAgg where Go's aggregation elimination
/// collapses the unique-keyed group to `Projection(1->)`; the join operator
/// and both scans — the compared access property — agree.
#[test]
fn the_recorded_explain_easy_shape_reads_both_tables_in_key_order() {
    let mut session = Session::new();
    session
        .run("create table t1 (c1 int primary key, c2 int, c3 int, index c2 (c2))")
        .unwrap();
    let plan = rows(
        &mut session,
        "EXPLAIN SELECT (SELECT COUNT(1) k FROM t1 s WHERE s.c1 = t1.c1 HAVING k != 0) FROM t1",
    );
    let operators: Vec<&str> = plan.iter().map(|row| row[0].as_str()).collect();
    assert!(
        operators[1].contains("MergeJoin"),
        "the decorrelated join must merge on the shared key order: {plan:?}"
    );
    let scans: Vec<&Vec<String>> = plan
        .iter()
        .filter(|row| row[0].contains("TableFullScan"))
        .collect();
    assert_eq!(scans.len(), 2, "both sides read the table: {plan:?}");
    for scan in scans {
        assert!(
            scan[4].contains("keep order:true"),
            "a merge-join side scans in key order: {scan:?}"
        );
    }
    assert!(
        !plan.iter().any(|row| row[0].contains("IndexFullScan")),
        "the covering c2 index is the Apply plan's pick, not the join's: {plan:?}"
    );
}
