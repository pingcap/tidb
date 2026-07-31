#![cfg(test)]

//! `ANALYZE TABLE` end to end: the statistics it publishes, and the `EXPLAIN`
//! numbers they change.
//!
//! Every expected value here is a CAPTURE from a real TiDB session over
//! `mockstore` (`rust/difftests/gorun`) on the same schema and the same rows,
//! not a number this engine produced and then froze. The whole risk of a
//! statistics tier is that a wrong histogram silently costs every later plan,
//! so the assertions are on the ESTIMATES Go produced, not merely on
//! `stats:pseudo` disappearing.

use crate::tests_support::*;
use crate::*;

/// The scan row's `estRows` and `operator info`, which is where the statistics
/// show up.
fn scan_row(session: &mut Session, sql: &str) -> (String, String) {
    let rows = row_text(session.run(sql));
    let scan = rows
        .iter()
        .find(|row| row[0].contains("Scan") || row[0].contains("Get"))
        .unwrap_or_else(|| panic!("no scan operator in the plan of `{sql}`: {rows:?}"));
    (scan[1].clone(), scan[4].clone())
}

/// The `estRows` of the plan's TOP operator, which is what a parent join or
/// aggregate would cost from.
fn top_est_rows(session: &mut Session, sql: &str) -> String {
    row_text(session.run(sql))[0][1].clone()
}

/// A ten-row table with a known distribution, analyzed.
///
/// Captured from `gorun` on `t(a INT, b VARCHAR(16), KEY ka(a), KEY kb(b))`
/// with `a` taking 1 once, 2 twice, 3 three times and 4 four times:
///
/// ```text
/// explain select * from t where a > 2            -- BEFORE analyze
///   TableReader_7        3333.33   root                data:Selection_6
///   └─Selection_6        3333.33   cop[tikv]           gt(test.t.a, 2)
///     └─TableFullScan_5  10000.00  cop[tikv] table:t   keep order:false, stats:pseudo
/// analyze table t
/// explain select * from t where a > 2            -- AFTER
///   TableReader_7        7.00      root                data:Selection_6
///   └─Selection_6        7.00      cop[tikv]           gt(test.t.a, 2)
///     └─TableFullScan_5  10.00     cop[tikv] table:t   keep order:false
/// explain select * from t where a = 4
///   TableReader_7        4.00      root                data:Selection_6
///   └─Selection_6        4.00      cop[tikv]           eq(test.t.a, 4)
///     └─TableFullScan_5  10.00     cop[tikv] table:t   keep order:false
/// select table_id, count, modify_count from mysql.stats_meta
///   117 | 10 | 0
/// ```
///
/// 7 and 4 are EXACT: with four distinct values and the default 500-entry
/// TopN, every value of `a` is a TopN entry, so Go's selectivity is a count of
/// rows rather than an interpolation. That is what makes them worth asserting
/// -- a histogram that merely "looks analyzed" would not reproduce them.
#[test]
fn analyze_publishes_the_row_count_and_the_distribution() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT, b VARCHAR(16), KEY ka(a), KEY kb(b))")
        .unwrap();
    session
        .run(
            "INSERT INTO t VALUES (1,'x'),(2,'y'),(2,'y'),(3,'z'),(3,'z'),(3,'z'),\
             (4,'w'),(4,'w'),(4,'w'),(4,'w')",
        )
        .unwrap();

    // Before: the pseudo table, exactly as Go reports it.
    assert_eq!(
        scan_row(&mut session, "EXPLAIN SELECT * FROM t"),
        (
            "10000.00".to_owned(),
            "keep order:false, stats:pseudo".to_owned()
        )
    );

    assert_eq!(
        session.run("ANALYZE TABLE t").unwrap(),
        StmtResult::Affected(0)
    );

    // After: the real row count, and no `stats:pseudo` -- Go drops the token
    // once a table has one initialized histogram.
    assert_eq!(
        scan_row(&mut session, "EXPLAIN SELECT * FROM t"),
        ("10.00".to_owned(), "keep order:false".to_owned())
    );
    assert_eq!(
        top_est_rows(&mut session, "EXPLAIN SELECT * FROM t"),
        "10.00"
    );

    // The estimates Go computes from the same histogram, every one of them a
    // `gorun` capture of TiDB's own `Selection` estRows on this data.
    for (predicate, tidb) in [
        ("a > 2", "7.00"),
        ("a >= 3", "7.00"),
        ("a < 3", "3.00"),
        ("a = 4", "4.00"),
        ("a = 1", "1.00"),
        ("b = 'w'", "4.00"),
    ] {
        assert_eq!(
            top_est_rows(
                &mut session,
                &format!("EXPLAIN SELECT * FROM t WHERE {predicate}")
            ),
            tidb,
            "TiDB estimates {tidb} rows for `{predicate}` on this table"
        );
    }

    // Statistics must not move a single row of the ANSWER.
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM t")),
        vec![vec!["10"]]
    );
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM t WHERE a > 2")),
        vec![vec!["7"]]
    );
}

/// An ANALYZED EMPTY table stays pseudo.
///
/// Captured -- and this is the one that looks wrong until you check it:
///
/// ```text
/// create table e (a int, key ka(a))
/// analyze table e
/// explain select * from e where a > 2
///   IndexReader_6        3333.33  root                            index:IndexRangeScan_5
///   └─IndexRangeScan_5   3333.33  cop[tikv] table:e, index:ka(a)  range:(2,+inf], keep order:false, stats:pseudo
/// select table_id, count, modify_count from mysql.stats_meta
///   117 | 0 | 0
/// ```
///
/// The `stats_meta` row exists and says zero, and Go still plans against the
/// PSEUDO row count: `pkg/planner/core/stats.GetStatsTable` returns
/// `PseudoTable` the moment `RealtimeCount == 0`. Taking the zero literally
/// would make every path cost nothing and the choice among them arbitrary.
#[test]
fn analyzing_an_empty_table_leaves_it_pseudo() {
    let mut session = Session::new();
    session.run("CREATE TABLE e (a INT, KEY ka(a))").unwrap();
    session.run("ANALYZE TABLE e").unwrap();

    let (est_rows, info) = scan_row(&mut session, "EXPLAIN SELECT * FROM e WHERE a > 2");
    assert!(
        info.contains("stats:pseudo"),
        "an analyzed empty table is still Go's PseudoTable, got `{info}`"
    );
    // 3333.33 is TiDB's own number above: the pseudo 10000 through the pseudo
    // `>` rate, NOT the zero `mysql.stats_meta` records.
    assert_eq!(est_rows, "3333.33");
}

/// A single-column UNIQUE index suppresses the TopN on itself AND on the
/// column it covers, which is Go's own rule -- captured:
///
/// ```text
/// create table u (a int unique, b int)
/// insert into u values (1,1),(2,2),(3,3),(4,4)
/// analyze table u
/// select table_id, is_index, hist_id, count(*) from mysql.stats_top_n group by ...
///   123 | 0 | 2 | 4          -- only column b (hist_id 2) has a TopN
/// explain select * from u where a = 2
///   Point_Get_1  1.00  root  table:u, index:a(a)
/// ```
///
/// A value that occurs at most once has no "top", so Go asks the builder for
/// none. Asserting the ESTIMATE rather than the absent TopN is the point: a
/// tier that built the TopN anyway would still answer 1.00 here, but would
/// have stored a list of ones for every unique column in the corpus.
#[test]
fn a_unique_column_estimates_one_row() {
    let mut session = Session::new();
    session.run("CREATE TABLE u (a INT UNIQUE, b INT)").unwrap();
    session
        .run("INSERT INTO u VALUES (1,1),(2,2),(3,3),(4,4)")
        .unwrap();
    session.run("ANALYZE TABLE u").unwrap();

    assert_eq!(
        top_est_rows(&mut session, "EXPLAIN SELECT * FROM u WHERE a = 2"),
        "1.00"
    );
    // The table's own count is still real: four rows, not the pseudo 10000.
    assert_eq!(scan_row(&mut session, "EXPLAIN SELECT * FROM u").0, "4.00");
}

/// Rows written AFTER an `ANALYZE` do not move its estimates.
///
/// Captured on `m(a INT, b INT, KEY ka(a))` analyzed at five rows and then
/// given three more:
///
/// ```text
/// explain select * from m where a > 2
///   TableReader_7        3.00  root               data:Selection_6
///   └─Selection_6        3.00  cop[tikv]          gt(test.m.a, 2)
///     └─TableFullScan_5  5.00  cop[tikv] table:m  keep order:false
/// ```
///
/// FIVE, not eight: the statistics describe the moment they were built. (Go's
/// `mysql.stats_meta` row does drift to 8/3 as the delta flush lands, but the
/// planner reads its loaded copy, which is what this asserts. This tier keeps
/// no delta at all, so it reports the same 5.)
#[test]
fn statistics_describe_the_moment_they_were_built() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE m (a INT, b INT, KEY ka(a))")
        .unwrap();
    session
        .run("INSERT INTO m VALUES (1,1),(2,2),(3,3),(4,4),(5,5)")
        .unwrap();
    session.run("ANALYZE TABLE m").unwrap();
    session
        .run("INSERT INTO m VALUES (6,6),(7,7),(8,8)")
        .unwrap();

    assert_eq!(scan_row(&mut session, "EXPLAIN SELECT * FROM m").0, "5.00");
    assert_eq!(
        top_est_rows(&mut session, "EXPLAIN SELECT * FROM m WHERE a > 2"),
        "3.00"
    );
    // The rows themselves are all eight -- an estimate is not a filter.
    assert_eq!(
        row_text(session.run("SELECT count(*) FROM m")),
        vec![vec!["8"]]
    );
}

/// `ANALYZE TABLE` names its table the way every other statement does, and
/// refuses a name that is not one.
#[test]
fn analyze_resolves_and_refuses_names() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT)").unwrap();
    session.run("INSERT INTO t VALUES (1),(2)").unwrap();

    // Schema-qualified and bare both reach the same table.
    session.run("ANALYZE TABLE test.t").unwrap();
    assert_eq!(scan_row(&mut session, "EXPLAIN SELECT * FROM t").0, "2.00");

    assert!(session.run("ANALYZE TABLE nosuch").is_err());

    session.run("CREATE VIEW v AS SELECT a FROM t").unwrap();
    assert!(
        session.run("ANALYZE TABLE v").is_err(),
        "a view has no rows of its own to analyze"
    );
}

/// A clause this engine does not implement is refused by name rather than
/// answered OK -- an `ANALYZE` that returns success without rebuilding the
/// histograms would leave the planner estimating from whatever was there
/// before while the client believes it just measured the table.
#[test]
fn an_unimplemented_analyze_clause_is_refused() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, KEY ka(a))").unwrap();
    session.run("INSERT INTO t VALUES (1),(2)").unwrap();

    for refused in [
        "ANALYZE TABLE t INDEX ka",
        "ANALYZE TABLE t COLUMNS a",
        "ANALYZE INCREMENTAL TABLE t INDEX ka",
    ] {
        assert!(
            session.run(refused).is_err(),
            "`{refused}` must refuse rather than answer OK"
        );
    }
    // ... and the refusals left the table unanalyzed, not half-analyzed.
    assert_eq!(
        scan_row(&mut session, "EXPLAIN SELECT * FROM t").1,
        "keep order:false, stats:pseudo"
    );
}
