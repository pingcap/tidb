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

/// `ANALYZE` inside an explicit transaction runs, and a `ROLLBACK` takes its
/// statistics back with it. TiDB's do NOT come back -- captured:
///
/// ```text
/// create table t (a int, key ka(a))
/// insert into t values (1),(2),(3)
/// begin
/// analyze table t
/// explain select * from t     -- IndexFullScan_6  3.00  (no stats:pseudo)
/// rollback
/// explain select * from t     -- IndexFullScan_6  3.00  STILL 3.00
/// ```
///
/// DIVERGENCE, named rather than papered over (see [`crate::analyze_arm`]):
/// TiDB's `ANALYZE` writes through an INTERNAL session, so its statistics are
/// not the rolling-back transaction's to discard. This tier runs it against
/// the catalog the statement sees, so they are. Making the WRITE escape the
/// transaction would also make the READ escape it, and sampling rows the
/// statement cannot see is the worse of the two errors: it would build a
/// histogram of a table state this session never observed.
///
/// The committed path is the one the scripts take, and it agrees with TiDB.
#[test]
fn analyze_inside_a_transaction_rolls_back_with_it() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, KEY ka(a))").unwrap();
    session.run("INSERT INTO t VALUES (1),(2),(3)").unwrap();

    session.run("BEGIN").unwrap();
    session.run("ANALYZE TABLE t").unwrap();
    // Inside the transaction the estimate is TiDB's.
    assert_eq!(scan_row(&mut session, "EXPLAIN SELECT * FROM t").0, "3.00");
    session.run("ROLLBACK").unwrap();
    assert_eq!(
        scan_row(&mut session, "EXPLAIN SELECT * FROM t").0,
        "10000.00",
        "the divergence above: TiDB keeps 3.00 here"
    );

    session.run("BEGIN").unwrap();
    session.run("ANALYZE TABLE t").unwrap();
    session.run("COMMIT").unwrap();
    assert_eq!(scan_row(&mut session, "EXPLAIN SELECT * FROM t").0, "3.00");
}

/// `Selectivity`'s two rules this port did not have: the DNF branch and the
/// one-row floor.
///
/// The fixture is Go's own `TestDNFCondSelectivity`
/// (`pkg/planner/cardinality/selectivity_test.go:1075`) -- same schema, same
/// eight rows, same two extra indexes, same `ANALYZE` -- and the estimates
/// below are captured from a real Go session on it (`rust/difftests/gorun`):
///
/// ```text
/// explain select * from dt where b > 7 or c < 4;
///   Selection_9  2.75  or(gt(test.dt.b, 7), lt(test.dt.c, 4))
/// explain select * from dt where d < 5 or b > 6;                       5.00
/// explain select * from dt where a > 8 or d < 4 or c > 7 or b < 5;     6.59
/// explain select * from dt where c = 4 and d = 4;                      1.00
/// explain select * from dt where c = 6 and d = 10 and b = 2;           1.00
/// explain select * from dt where c > 100;                              1.00
/// ```
///
/// `2.75 / 8 = 0.34375`, `5.00 / 8 = 0.625` and `6.59 / 8 = 0.82421875` are
/// exactly the three values Go's own `TestDNFCondSelectivity` testdata holds,
/// so this pins the algorithm and not just the rendering. A port with no DNF
/// branch reads 6.40 for all three -- the flat `selectionFactor` of 0.8.
///
/// The equality rows are the floor: three independent point conditions on an
/// eight-row table multiply to about 8/512 of a row, and the source refuses
/// to believe in less than one.
///
/// KNOWN REMAINING GAP, measured: `a < 8 and (b > 10 or c < 3 or b > 4) and
/// a > 2` is 2.50 in Go and 4.00 here. That is NOT the DNF branch -- it is
/// `plan_trace::selection`'s `Est::Inherit`, which keeps the access path's
/// own estimate whenever the path consumed a condition, while Go recomputes
/// the whole `Selection` as `RealtimeCount * Selectivity(allConds)`.
#[test]
fn analyzed_selectivity_estimates_a_disjunction_and_never_drops_below_one_row() {
    let mut session = Session::new();
    session
        .run("create table dt(a int, b int, c int, d int, index idx(a, b, c, d))")
        .unwrap();
    session
        .run(
            "insert into dt value(1,5,4,4),(3,4,1,8),(4,2,6,10),(6,7,2,5),\
             (7,1,4,9),(8,9,8,3),(9,1,9,1),(10,6,6,2)",
        )
        .unwrap();
    session.run("alter table dt add index (b)").unwrap();
    session.run("alter table dt add index (d)").unwrap();
    session.run("analyze table dt").unwrap();

    let selection_rows = |session: &mut Session, sql: &str| -> String {
        let rows = row_text(session.run(sql));
        rows.iter()
            .find(|row| row[0].contains("Selection"))
            .unwrap_or_else(|| panic!("no Selection in the plan of `{sql}`: {rows:?}"))[1]
            .clone()
    };

    for (sql, expected) in [
        ("explain select * from dt where b > 7 or c < 4", "2.75"),
        ("explain select * from dt where d < 5 or b > 6", "5.00"),
        (
            "explain select * from dt where a > 8 or d < 4 or c > 7 or b < 5",
            "6.59",
        ),
        ("explain select * from dt where c = 4 and d = 4", "1.00"),
        (
            "explain select * from dt where c = 6 and d = 10 and b = 2",
            "1.00",
        ),
        ("explain select * from dt where c > 100", "1.00"),
    ] {
        assert_eq!(selection_rows(&mut session, sql), expected, "{sql}");
    }
}

/// Go `getColumnRowCount`'s sort-key conversion
/// (`pkg/planner/cardinality/row_count_column.go:126-132`): a string range
/// endpoint is replaced by its COLLATION SORT KEY before it is encoded,
/// compared, or located in a bucket.
///
/// `ANALYZE` writes the sort key as the bucket bound under a new collation,
/// so a raw endpoint locates the wrong bucket. Nothing smaller than a
/// MULTI-BUCKET histogram sees it: an eight-row table is answered entirely
/// out of the TopN, which compares encoded values and agrees either way.
/// That is why this fixture is 300 rows of deliberately mixed case, whose
/// byte order and `utf8mb4_unicode_ci` order disagree completely.
///
/// Every expectation is TiDB's own printed estRows for the same statement on
/// the same 300 rows, captured with `gorun`.
#[test]
fn a_case_insensitive_column_estimates_off_the_collation_sort_key() {
    let mut session = Session::new();
    session
        .run("create table c4 (s varchar(32) collate utf8mb4_unicode_ci)")
        .unwrap();
    let values: Vec<String> = (0..300)
        .map(|i: usize| {
            let letters = b"abcdefghijklmnopqrstuvwxyz";
            let word = format!(
                "{}{}{}",
                letters[i % 26] as char,
                letters[(i / 26) % 26] as char,
                i % 7
            );
            let word = if i % 2 == 1 {
                word.to_uppercase()
            } else {
                word
            };
            format!("('{word}')")
        })
        .collect();
    session
        .run(&format!("insert into c4 values {}", values.join(", ")))
        .unwrap();
    session.run("analyze table c4").unwrap();

    let selection = |session: &mut Session, sql: &str| -> String {
        let rows = row_text(session.run(sql));
        rows.iter()
            .find(|row| row[0].contains("Selection"))
            .unwrap_or_else(|| panic!("no Selection in the plan of `{sql}`: {rows:?}"))[1]
            .clone()
    };

    for (sql, expected) in [
        // The four that MOVED. Raw endpoints put `'mm3'` past the end of the
        // histogram's byte order (every upper-cased bound sorts before every
        // lower-cased one), which printed 43.00 / 258.00 / 22.00 / 4.00.
        ("explain select * from c4 where s > 'mm3'", "144.00"),
        ("explain select * from c4 where s < 'mm3'", "157.00"),
        (
            "explain select * from c4 where s >= 'ca0' and s <= 'pz6'",
            "166.00",
        ),
        (
            "explain select * from c4 where s between 'FA1' and 'ka2'",
            "61.00",
        ),
        // The two the TopN already answered, which is why an eight-row
        // fixture could not have caught this.
        ("explain select * from c4 where s = 'AA0'", "1.00"),
        ("explain select * from c4 where s > 'zz9'", "1.00"),
    ] {
        assert_eq!(selection(&mut session, sql), expected, "{sql}");
    }
}
