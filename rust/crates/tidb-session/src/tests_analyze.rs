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

use tidb_stats_handle::catalog_statistics::CatalogStatistics;
use tidb_executor::stmt_ctx_build::ResetContextOfStmt;
use crate::tests_support::*;
use crate::*;
use tidb_infoschema::TableEntry;

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
/// `(partition, name, is_index, distinct_count, null_count)` per
/// `SHOW STATS_HISTOGRAMS` row of `table`: what Go's statistics tests read
/// once a table is analyzed.
fn stats_histograms(session: &mut Session, table: &str) -> Vec<[String; 5]> {
    row_text(session.run(&format!(
        "SHOW STATS_HISTOGRAMS WHERE table_name = '{table}'"
    )))
    .into_iter()
    .map(|row| {
        [
            row[2].clone(),
            row[3].clone(),
            row[4].clone(),
            row[6].clone(),
            row[7].clone(),
        ]
    })
    .collect()
}

/// The column names `SHOW STATS_HISTOGRAMS` lists for `table`'s own (not a
/// partition's) statistics.
fn histogram_columns(session: &mut Session, table: &str) -> std::collections::BTreeSet<String> {
    stats_histograms(session, table)
        .into_iter()
        .filter(|row| row[2] == "0" && (row[0].is_empty() || row[0] == "global"))
        .map(|row| row[1].clone())
        .collect()
}

/// `(partition, row_count)` per `SHOW STATS_META` row of `table`.
fn stats_meta_rows(session: &mut Session, table: &str) -> Vec<(String, String)> {
    row_text(session.run(&format!("SHOW STATS_META WHERE table_name = '{table}'")))
        .into_iter()
        .map(|row| (row[2].clone(), row[5].clone()))
        .collect()
}

fn names(list: &[&str]) -> std::collections::BTreeSet<String> {
    list.iter().map(|name| (*name).to_owned()).collect()
}

#[test]
fn first_analyze_without_cached_statistics_collects_small_table_distribution() {
    let mut session = Session::new();
    session.run("CREATE TABLE fresh_stats (a INT)").unwrap();
    session
        .run("INSERT INTO fresh_stats VALUES (1),(2),(2)")
        .unwrap();
    session
        .run("ANALYZE TABLE fresh_stats ALL COLUMNS")
        .unwrap();
    let stats_handle = session.stats_handle();
    session
        .with_committed_catalog_mut(|catalog| {
            let Some(TableEntry::Kv(table)) = catalog.table_in("test", "fresh_stats") else {
                panic!("fresh_stats is not a stored table");
            };
            let stats = catalog
                .table_statistics_in(&stats_handle, table.table_id())
                .unwrap();
            let column = stats.hist_coll.stable_columns().remove(0);
            let column = column.read().unwrap();
            assert_eq!(column.total_row_count(), 3.0);
            assert_eq!(column.histogram.ndv, 2);
            Ok(())
        })
        .unwrap();
}

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
    // Go testkit's store (stats lease disabled) holds every histogram the
    // ANALYZE wrote fully loaded, so no item is marked partial (testkit
    // probe; a server with a lease would mark `kb` allEvicted).
    let expected = "keep order:false";
    assert_eq!(
        scan_row(&mut session, "EXPLAIN SELECT * FROM t"),
        ("10.00".to_owned(), expected.to_owned())
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

/// Go analyzes each physical partition under static pruning and does not
/// merge a logical-table histogram. Switching that same session back to the
/// default dynamic pruning therefore sees pseudo global statistics and keeps
/// the narrowed PRIMARY-index range, while static planning turns each point
/// range into a partition-local Batch Point Get.
#[test]
fn static_partition_analyze_keeps_statistics_on_physical_partitions() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    session
        .run(
            "CREATE TABLE t (a VARCHAR(255), b INT PRIMARY KEY NONCLUSTERED, KEY (a)) \
             PARTITION BY KEY (b) PARTITIONS 3",
        )
        .unwrap();
    session
        .run("INSERT INTO t VALUES ('Ab',1),('abc',2),('BC',3),('AC',4),('BA',5),('cda',6)")
        .unwrap();
    session.run("ANALYZE TABLE t").unwrap();

    // Static mode analyzes the partitions alone (testkit probe): no global
    // `stats_meta` row, and every partition has its PRIMARY histogram.
    let meta = stats_meta_rows(&mut session, "t");
    assert!(
        meta.iter().all(|(partition, _)| partition != "global"),
        "static ANALYZE must not invent global table statistics: {meta:?}"
    );
    let histograms = stats_histograms(&mut session, "t");
    for partition in ["p0", "p1", "p2"] {
        assert!(
            histograms
                .iter()
                .any(|row| row[0] == partition && row[1] == "PRIMARY" && row[2] == "1"),
            "partition {partition} was not analyzed: {histograms:?}"
        );
    }

    let static_plan =
        row_text(session.run("EXPLAIN SELECT * FROM t WHERE b IN (1,2) AND a LIKE '%a%'"));
    assert!(
        static_plan
            .iter()
            .flatten()
            .any(|cell| cell.contains("Batch_Point_Get")),
        "static partition planning must retain the point ranges: {static_plan:?}"
    );

    session
        .run("SET @@tidb_partition_prune_mode = DEFAULT")
        .unwrap();
    let dynamic_plan = row_text(session.run("EXPLAIN SELECT * FROM t WHERE b IN (1,2)"));
    assert!(
        dynamic_plan
            .iter()
            .flatten()
            .any(|cell| cell.contains("IndexRangeScan")),
        "dynamic pruning must retain the PRIMARY-index range: {dynamic_plan:?}"
    );
    assert!(
        dynamic_plan
            .iter()
            .flatten()
            .any(|cell| cell.contains("stats:pseudo")),
        "without merged global statistics Go reports the dynamic path as pseudo: {dynamic_plan:?}"
    );

    session.run("ANALYZE TABLE t").unwrap();
    assert!(
        stats_meta_rows(&mut session, "t").contains(&("global".to_owned(), "6".to_owned())),
        "dynamic ANALYZE publishes global table statistics"
    );
    let analyzed_dynamic_plan = row_text(session.run("EXPLAIN SELECT * FROM t WHERE b IN (1,2)"));
    assert!(
        analyzed_dynamic_plan
            .iter()
            .flatten()
            .all(|cell| !cell.contains("stats:pseudo")),
        "dynamic ANALYZE must publish usable global statistics: {analyzed_dynamic_plan:?}"
    );
}

#[test]
fn analyze_persists_effective_column_list_and_reuses_raw_options() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT PRIMARY KEY, b INT, c INT, KEY kb(b))")
        .unwrap();
    session.run("INSERT INTO t VALUES (1,2,3),(2,3,4)").unwrap();

    session
        .run("ANALYZE TABLE t COLUMNS c WITH 7 BUCKETS")
        .unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![
            vec![
                "Warning",
                "1105",
                "Columns a,b are missing in ANALYZE but their stats are needed for calculating stats for indexes/primary key/extended stats",
            ],
            vec!["Note", "1105", "Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\""],
        ]
    );
    // Go persists the options in `mysql.analyze_options`.
    let saved = |session: &mut Session| {
        row_text(session.run(
            "SELECT o.buckets, o.column_choice, o.column_ids FROM mysql.analyze_options o \
             JOIN information_schema.tables t ON o.table_id = t.tidb_table_id \
             WHERE t.table_schema = 'test' AND t.table_name = 't'",
        ))
    };
    assert_eq!(saved(&mut session), [["7", "LIST", "1,2,3"]]);
    session.run("ANALYZE TABLE t").unwrap();
    assert_eq!(
        saved(&mut session)[0][0],
        "7",
        "a statement without BUCKETS reuses the persisted raw value"
    );
    session
        .run("SET GLOBAL tidb_persist_analyze_options = OFF")
        .unwrap();
    session
        .run("ANALYZE TABLE t ALL COLUMNS WITH 9 BUCKETS")
        .unwrap();
    assert_eq!(saved(&mut session), [["7", "LIST", "1,2,3"]]);
    session
        .run("SET GLOBAL tidb_persist_analyze_options = ON")
        .unwrap();
}

#[test]
fn static_partition_analyze_inherits_and_updates_only_named_partition_options() {
    let mut session = Session::new();
    session
        .run("SET @@tidb_partition_prune_mode = 'static'")
        .unwrap();
    session
        .run(
            "CREATE TABLE p (a INT, b INT, KEY kb(b)) \
             PARTITION BY RANGE (a) (\
               PARTITION p0 VALUES LESS THAN (10),\
               PARTITION p1 VALUES LESS THAN MAXVALUE)",
        )
        .unwrap();
    session.run("INSERT INTO p VALUES (1,1),(11,11)").unwrap();
    session.run("ANALYZE TABLE p WITH 7 BUCKETS").unwrap();
    // `(partition, buckets)` per `mysql.analyze_options` row of p, the table
    // itself under the empty name.
    let options = |session: &mut Session| {
        row_text(session.run(
            "SELECT t.partition_name, o.buckets FROM mysql.analyze_options o JOIN (\
               SELECT tidb_table_id id, '' partition_name FROM information_schema.tables \
                 WHERE table_schema = 'test' AND table_name = 'p' \
               UNION ALL SELECT tidb_partition_id, partition_name FROM information_schema.partitions \
                 WHERE table_schema = 'test' AND table_name = 'p') t \
             ON o.table_id = t.id ORDER BY t.partition_name",
        ))
    };
    assert_eq!(
        options(&mut session),
        [["", "7"], ["p0", "7"], ["p1", "7"]],
        "whole-table ANALYZE saves every static physical row"
    );

    session.run("INSERT INTO p VALUES (2,2),(12,12)").unwrap();
    session
        .run("ANALYZE TABLE p PARTITION p0 WITH 9 BUCKETS")
        .unwrap();
    assert_eq!(
        stats_meta_rows(&mut session, "p"),
        [
            ("p0".to_owned(), "2".to_owned()),
            ("p1".to_owned(), "1".to_owned())
        ],
        "a named static ANALYZE does not refresh an unrequested partition"
    );
    assert_eq!(options(&mut session), [["", "7"], ["p0", "9"], ["p1", "7"]]);
}

/// The row decoder used by `ANALYZE` evaluates generated columns instead of
/// refusing the table or sampling the stored placeholder for a virtual one.
#[test]
fn analyze_publishes_stored_but_not_virtual_generated_column_histograms() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE generated_stats (\
                 a INT, \
                 virtual_value INT AS (a + 1), \
                 stored_value INT AS (a + 2) STORED\
             )",
        )
        .unwrap();
    session
        .run("INSERT INTO generated_stats (a) VALUES (1),(2),(2),(3)")
        .unwrap();

    assert_eq!(
        session.run("ANALYZE TABLE generated_stats").unwrap(),
        StmtResult::Affected(0)
    );
    // Go testkit: a histogram for `a` and the stored generated column, none
    // for the virtual one.
    assert_eq!(
        histogram_columns(&mut session, "generated_stats"),
        names(&["a", "stored_value"])
    );
    assert_eq!(
        stats_meta_rows(&mut session, "generated_stats"),
        [(String::new(), "4".to_owned())]
    );
}

/// Pinned Go warns when predicate-column storage has no row for the table,
/// even though mandatory index columns still make the analysis executable.
#[test]
fn analyze_predicate_columns_without_collected_usage_warns() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, KEY idx_a(a))").unwrap();

    assert_eq!(
        session.run("ANALYZE TABLE t PREDICATE COLUMNS").unwrap(),
        StmtResult::Affected(0)
    );
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![
            vec![
                "Warning",
                "1105",
                "No predicate column has been collected yet for table test.t, so only indexes and the columns composing the indexes will be analyzed",
            ],
            vec!["Note", "1105", "Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\""],
        ]
    );
}

#[test]
fn analyze_default_predicate_columns_follow_usage_and_mandatory_index_columns() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE t (a INT, b INT, c INT, KEY ib(b))")
        .unwrap();
    session
        .run("INSERT INTO t VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    session
        .run("SET GLOBAL tidb_analyze_column_options = 'PREDICATE'")
        .unwrap();
    // Go records a predicate column when a query filters on it, and
    // `h.DumpColStatsUsageToKV()` persists it for ANALYZE to read; the
    // index column is mandatory whatever the usage says (testkit probe).
    session.run("SELECT * FROM t WHERE a > 0").unwrap();
    session.dump_col_stats_usage_for_test();
    session.run("ANALYZE TABLE t").unwrap();
    assert_eq!(histogram_columns(&mut session, "t"), names(&["a", "b"]));

    session.run("SELECT * FROM t WHERE c > 0").unwrap();
    session.dump_col_stats_usage_for_test();
    session.run("ANALYZE TABLE t").unwrap();
    assert_eq!(
        histogram_columns(&mut session, "t"),
        names(&["a", "b", "c"])
    );
    session
        .run("SET GLOBAL tidb_analyze_column_options = 'ALL'")
        .unwrap();
}

#[test]
fn analyze_empty_predicate_usage_keeps_only_go_mandatory_columns() {
    let mut session = Session::new();
    session.run("CREATE TABLE plain (a INT, b INT)").unwrap();
    session
        .run("CREATE TABLE indexed (a INT, b INT, KEY iab(a,b))")
        .unwrap();
    session
        .run("CREATE TABLE primary_key (a INT, b INT, c INT, PRIMARY KEY(a,b))")
        .unwrap();
    session.run("INSERT INTO plain VALUES (1,1)").unwrap();
    session.run("INSERT INTO indexed VALUES (1,1)").unwrap();
    session
        .run("INSERT INTO primary_key VALUES (1,1,1)")
        .unwrap();
    session
        .run("SET GLOBAL tidb_analyze_column_options = 'PREDICATE'")
        .unwrap();
    // No predicate column was ever recorded: Go analyzes only the index
    // columns, so a table without an index gets no column histogram.
    for (table, expected) in [
        ("plain", names(&[])),
        ("indexed", names(&["a", "b"])),
        ("primary_key", names(&["a", "b"])),
    ] {
        session.run(&format!("ANALYZE TABLE {table}")).unwrap();
        assert_eq!(histogram_columns(&mut session, table), expected, "{table}");
    }
    session
        .run("SET GLOBAL tidb_analyze_column_options = 'ALL'")
        .unwrap();
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

/// A zero-count table plans from a FRESH `PseudoTable`, not from the cached
/// table with its row count patched.
///
/// Captured from a Go tidb-server over a table analyzed while empty and then
/// written to, whose `mysql.stats_meta` row reads `count 0, modify_count 75`:
///
/// ```text
/// explain format='brief' select * from t where k > 5
///   TableReader      3333.33   data:Selection
///   └─Selection      3333.33   gt(acc2.t.k, 5)
///     └─TableFullScan 10000.00 keep order:false, stats:pseudo
/// explain format='brief' select k, count(*) from t group by k
///   Projection 8000.00 / HashAgg 8000.00 / IndexReader 8000.00 / HashAgg 8000.00
///   └─IndexFullScan 10000.00
/// explain format='brief' select * from t where k = 1 limit 5
///   IndexLookUp 5.00 limit embedded(offset:0, count:5)
///   ├─Limit(Build) 5.00 / IndexRangeScan 5.00
///   └─TableRowIDScan(Probe) 5.00
/// ```
///
/// `pkg/planner/core/stats.GetStatsTable` rule 2 returns
/// `statistics.PseudoTable` -- `RealtimeCount` 10000, `ModifyCount` 0 and
/// uninitialized pseudo histograms -- so `EstimateColumnNDV` answers
/// `10000 * 0.8` and the pseudo index estimate reads 10000 rows. Keeping the
/// cached zero-NDV histograms and the zero count with `modify_count` made the
/// NDV 1 and every index-derived estimate 1.00.
#[test]
fn a_zero_count_table_plans_from_a_fresh_pseudo_table() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE z (id INT PRIMARY KEY, v INT, k INT, KEY ik(k))")
        .unwrap();
    session.run("ANALYZE TABLE z").unwrap();
    session
        .run("INSERT INTO z VALUES (1,1,1),(2,2,2),(3,3,3)")
        .unwrap();
    session.run("DELETE FROM z").unwrap();
    session.run("FLUSH STATS_DELTA test.z").unwrap();
    let meta = row_text(session.run("SHOW STATS_META WHERE table_name = 'z'"));
    assert_eq!(meta.len(), 1, "{meta:?}");
    assert_eq!(meta[0][5], "0", "the row count stays zero: {meta:?}");
    // A Go server's SHOW STATS_META answers its stats cache, which the
    // flush does not refresh; the durable row has the delta (oracle:
    // cached 0, `mysql.stats_meta` 6).
    assert_eq!(
        row_text(session.run(
            "SELECT count, modify_count FROM mysql.stats_meta WHERE table_id = \
             (SELECT tidb_table_id FROM information_schema.tables \
             WHERE table_schema = 'test' AND table_name = 'z')"
        )),
        [["0", "6"]]
    );

    let plan = |session: &mut Session, sql: &str| {
        row_text(session.run(sql))
            .into_iter()
            .map(|row| format!("{} {}", row[0], row[1]))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM z WHERE k > 5"
        ),
        [
            "TableReader 3333.33",
            "└─Selection 3333.33",
            "  └─TableFullScan 10000.00",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT k, COUNT(*) FROM z GROUP BY k"
        ),
        [
            "Projection 8000.00",
            "└─HashAgg 8000.00",
            "  └─IndexReader 8000.00",
            "    └─HashAgg 8000.00",
            "      └─IndexFullScan 10000.00",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM z WHERE k = 1 LIMIT 5"
        ),
        [
            "IndexLookUp 5.00",
            "├─Limit(Build) 5.00",
            "│ └─IndexRangeScan 5.00",
            "└─TableRowIDScan(Probe) 5.00",
        ]
    );
    // The same capture's joins: the limit's expected count reaches the
    // build side through 10000-row pseudo estimates on both sides.
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM z a JOIN z b ON a.id = b.k LIMIT 5"
        ),
        [
            "Limit 5.00",
            "└─Projection 5.00",
            "  └─IndexJoin 5.00",
            "    ├─IndexLookUp(Build) 4.00",
            "    │ ├─IndexFullScan(Build) 4.00",
            "    │ └─TableRowIDScan(Probe) 4.00",
            "    └─TableReader(Probe) 4.00",
            "      └─TableRangeScan 4.00",
        ]
    );
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM z a JOIN z b ON a.id = b.k WHERE a.k = 2"
        ),
        [
            "IndexHashJoin 12.50",
            "├─IndexLookUp(Build) 10.00",
            "│ ├─IndexRangeScan(Build) 10.00",
            "│ └─TableRowIDScan(Probe) 10.00",
            "└─IndexLookUp(Probe) 12.50",
            "  ├─Selection(Build) 12.50",
            "  │ └─IndexRangeScan 12.51",
            "  └─TableRowIDScan(Probe) 12.50",
        ]
    );
}

/// `GetStatsTable` judges `IsOutdated` on the table it plans from: the
/// determinate-objective copy has `ModifyCount` 0, so it is never outdated.
///
/// Captured from Go's `testkit` over the same rows (10 rows, `ANALYZE`, 20
/// more rows, stats delta flushed: `stats_meta` count 30, modify_count 20):
///
/// ```text
/// set tidb_enable_pseudo_for_outdated_stats = 1;
/// explain format='brief' select * from o where b > 5
///   TableReader 10.00 / Selection 10.00 / TableFullScan 30.00 stats:pseudo
/// set tidb_opt_objective = 'determinate';
/// explain format='brief' select * from o where b > 5
///   TableReader 5.00 / Selection 5.00 / TableFullScan 10.00 (no pseudo)
/// ```
///
/// Deciding outdatedness on the cached table (modify_count 20 over an
/// analyzed count of 10) marked the determinate plan pseudo as well.
#[test]
fn outdated_statistics_are_judged_on_the_determinate_copy() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE o (a INT, b INT, KEY ia(a))")
        .unwrap();
    let rows = |range: std::ops::RangeInclusive<i32>| {
        range
            .map(|value| format!("({value},{value})"))
            .collect::<Vec<_>>()
            .join(",")
    };
    session
        .run(&format!("INSERT INTO o VALUES {}", rows(1..=10)))
        .unwrap();
    session.run("ANALYZE TABLE o ALL COLUMNS").unwrap();
    session
        .run(&format!("INSERT INTO o VALUES {}", rows(11..=30)))
        .unwrap();
    session.run("FLUSH STATS_DELTA test.o").unwrap();
    // Go's test reloads the cache after the flush (`h.Update`).
    session.update_stats_cache().unwrap();
    let meta = row_text(session.run("SHOW STATS_META WHERE table_name = 'o'"));
    assert_eq!(
        (meta[0][4].as_str(), meta[0][5].as_str()),
        ("20", "30"),
        "{meta:?}"
    );

    let plan = |session: &mut Session| {
        row_text(session.run("EXPLAIN FORMAT='brief' SELECT * FROM o WHERE b > 5"))
            .into_iter()
            .map(|row| format!("{} {} {}", row[0], row[1], row[4]))
            .collect::<Vec<_>>()
    };
    session
        .run("SET tidb_enable_pseudo_for_outdated_stats = 1")
        .unwrap();
    assert_eq!(
        plan(&mut session),
        [
            "TableReader 10.00 data:Selection",
            "└─Selection 10.00 gt(test.o.b, 5)",
            "  └─TableFullScan 30.00 keep order:false, stats:pseudo",
        ]
    );
    session
        .run("SET tidb_opt_objective = 'determinate'")
        .unwrap();
    // Go testkit holds the analyzed index loaded (no stats lease), so no
    // partial marker; a server would leave the unused `ia` evicted.
    let scan = "  └─TableFullScan 10.00 keep order:false";
    assert_eq!(
        plan(&mut session),
        [
            "TableReader 5.00 data:Selection",
            "└─Selection 5.00 gt(test.o.b, 5)",
            scan,
        ]
    );
}

/// Determinate mode plans an emptied table from its ANALYZE-time count: the
/// copy's `RealtimeCount` is the analyzed 10, so rule 2 does not apply and
/// rule 3 finds initialized statistics.
///
/// Captured from Go's `testkit` (10 rows, `ANALYZE`, `DELETE`, flush:
/// `stats_meta` count 0, modify_count 10):
///
/// ```text
/// explain format='brief' select * from d where b > 5
///   TableReader 3333.33 / Selection 3333.33 / TableFullScan 10000.00 stats:pseudo
/// set tidb_opt_objective = 'determinate';
/// explain format='brief' select * from d where b > 5
///   TableReader 5.00 / Selection 5.00 / TableFullScan 10.00 (no pseudo)
/// explain format='brief' select a, count(*) from d group by a
///   Projection 10.00 / StreamAgg 10.00 / IndexReader 10.00
///   └─IndexFullScan 10.00 keep order:true (no pseudo)
/// ```
///
/// The cached object folds rule 2 into its `pseudo` bit for the cached zero
/// count; carrying that bit onto the copy marked the determinate plan pseudo.
#[test]
fn determinate_mode_plans_an_emptied_table_from_its_analyze_count() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE d (a INT, b INT, KEY ia(a))")
        .unwrap();
    session
        .run("INSERT INTO d VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10)")
        .unwrap();
    session.run("ANALYZE TABLE d ALL COLUMNS").unwrap();
    session.run("DELETE FROM d").unwrap();
    session.run("FLUSH STATS_DELTA test.d").unwrap();
    // Go's tests follow a flush with `h.Update`; until then SHOW STATS_META
    // answers the cached counts.
    session.update_stats_cache().unwrap();
    let meta = row_text(session.run("SHOW STATS_META WHERE table_name = 'd'"));
    assert_eq!(
        (meta[0][4].as_str(), meta[0][5].as_str()),
        ("10", "0"),
        "{meta:?}"
    );
    // The cluster loader (`table_statistics_from_table_schema`) bakes rule 2
    // into the planner copy's `pseudo` bit for a zero count.

    let plan = |session: &mut Session, sql: &str| {
        row_text(session.run(sql))
            .into_iter()
            .map(|row| format!("{} {} {}", row[0], row[1], row[4]))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM d WHERE b > 5"
        ),
        [
            "TableReader 3333.33 data:Selection",
            "└─Selection 3333.33 gt(test.d.b, 5)",
            "  └─TableFullScan 10000.00 keep order:false, stats:pseudo",
        ]
    );
    session
        .run("SET tidb_opt_objective = 'determinate'")
        .unwrap();
    assert_eq!(
        plan(
            &mut session,
            "EXPLAIN FORMAT='brief' SELECT * FROM d WHERE b > 5"
        ),
        [
            "TableReader 5.00 data:Selection",
            "└─Selection 5.00 gt(test.d.b, 5)",
            "  └─TableFullScan 10.00 keep order:false",
        ]
    );
    let grouped = plan(
        &mut session,
        "EXPLAIN FORMAT='brief' SELECT a, COUNT(*) FROM d GROUP BY a",
    );
    assert_eq!(grouped.len(), 4, "{grouped:?}");
    assert!(grouped[0].starts_with("Projection 10.00"), "{grouped:?}");
    assert!(grouped[1].starts_with("└─StreamAgg 10.00"), "{grouped:?}");
    assert!(
        grouped[2].starts_with("  └─IndexReader 10.00"),
        "{grouped:?}"
    );
    assert_eq!(
        grouped[3], "    └─IndexFullScan 10.00 keep order:true",
        "{grouped:?}"
    );
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

/// Stats v2 treats an ordinary index target as a request for the normal full
/// sampling task, exactly as Go's `buildAnalyzeFullSamplingTask` does.
#[test]
fn ordinary_index_target_collects_all_statistics() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, KEY ka(a))").unwrap();
    session.run("INSERT INTO t VALUES (1),(2)").unwrap();

    session.run("ANALYZE TABLE t INDEX ka").unwrap();
    assert_eq!(
        warnings_of(&session),
        vec![
            (
                1105,
                "The version 2 would collect all statistics not only the selected indexes".to_owned(),
            ),
            (1105, "Analyze use auto adjusted sample rate 1.000000 for table test.t, reason to use this rate is \"use min(1, 110000/10000) as the sample-rate=1\"".to_owned()),
        ]
    );
    assert_eq!(scan_row(&mut session, "EXPLAIN SELECT * FROM t").0, "2.00");

    let missing = session
        .run("ANALYZE TABLE t INDEX missing")
        .expect_err("Go rejects a named index that is not public metadata");
    assert!(missing
        .to_string()
        .contains("Index 'missing' in field list does not exist in table 't'"));

    assert!(
        session.run("ANALYZE INCREMENTAL TABLE t INDEX ka").is_err(),
        "the separate incremental v2 gap remains an explicit refusal"
    );
}

/// Go removes a special GLOBAL prefix index from the ordinary column-sampling
/// task and analyzes its logical-table keyspace through an independent index
/// task. That task replaces only the index item: it must not overwrite the
/// existing table row count, and it cannot be scoped to one partition.
#[test]
fn special_global_index_uses_the_independent_analyze_task() {
    let mut session = Session::new();
    session
        .run(
            "CREATE TABLE only_special (a INT, b VARCHAR(16), UNIQUE KEY gb(b(2)) GLOBAL) \
             PARTITION BY HASH(a) PARTITIONS 2",
        )
        .unwrap();
    session
        .run("INSERT INTO only_special VALUES (1,'aa')")
        .unwrap();
    session.run("ANALYZE TABLE only_special INDEX").unwrap();
    assert!(
        stats_histograms(&mut session, "only_special").is_empty(),
        "pinned Go's empty-name all-special branch creates no task"
    );

    session
        .run(
            "CREATE TABLE sg (a INT, b VARCHAR(16), UNIQUE KEY gb(b(2)) GLOBAL) \
             PARTITION BY HASH(a) PARTITIONS 2",
        )
        .unwrap();
    session
        .run("INSERT INTO sg VALUES (1,'aa'),(2,'bb'),(3,'cc'),(4,'dd')")
        .unwrap();

    session.run("ANALYZE TABLE sg").unwrap();
    session.run("INSERT INTO sg VALUES (5,'ee')").unwrap();
    session.run("ANALYZE TABLE sg INDEX gb").unwrap();
    assert!(warnings_of(&session).is_empty());
    // Go testkit: the independent index task keeps the global count of the
    // whole-table ANALYZE (4) and publishes gb over all five rows.
    let meta = stats_meta_rows(&mut session, "sg");
    assert!(
        meta.contains(&("global".to_owned(), "4".to_owned())),
        "an independent index task preserves existing stats_meta.count: {meta:?}"
    );
    let gb = stats_histograms(&mut session, "sg")
        .into_iter()
        .find(|row| row[0] == "global" && row[1] == "gb" && row[2] == "1")
        .expect("the independent task publishes gb");
    assert_eq!((gb[3].as_str(), gb[4].as_str()), ("5", "0"));
    let topn = row_text(session.run(
        "SHOW STATS_TOPN WHERE table_name = 'sg' AND partition_name = 'global' \
         AND column_name = 'gb'",
    ));
    assert_eq!(
        topn.iter()
            .map(|row| row[6].parse::<i64>().unwrap())
            .sum::<i64>(),
        5
    );

    let error = session
        .run("ANALYZE TABLE sg PARTITION p0 INDEX gb")
        .expect_err("Go cannot scope a global-index task to one partition");
    assert!(error
        .to_string()
        .contains("Analyze global index 'gb' can't work with analyze specified partitions"));
}

/// `ANALYZE` inside an explicit transaction runs through the process-wide
/// statistics handle, so a later `ROLLBACK` does not take its statistics back.
/// Captured from TiDB:
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
#[test]
fn analyze_inside_a_transaction_survives_rollback() {
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, KEY ka(a))").unwrap();
    session.run("INSERT INTO t VALUES (1),(2),(3)").unwrap();
    let mut peer = Session::with_catalog(session.shared_catalog());

    session.run("BEGIN").unwrap();
    session.run("ANALYZE TABLE t").unwrap();
    // The analyzing transaction and another session both see the stats-handle
    // publication before the user transaction ends.
    assert_eq!(scan_row(&mut session, "EXPLAIN SELECT * FROM t").0, "3.00");
    assert_eq!(scan_row(&mut peer, "EXPLAIN SELECT * FROM t").0, "3.00");
    session.run("ROLLBACK").unwrap();
    assert_eq!(scan_row(&mut session, "EXPLAIN SELECT * FROM t").0, "3.00");
    assert_eq!(scan_row(&mut peer, "EXPLAIN SELECT * FROM t").0, "3.00");

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

/// Go's statistics loader publishes physical-access requests to the shared
/// domain cache after the statement that requested them. The next statement
/// observes the load even when it comes from another connection, and later
/// statement boundaries do not apply the same request again.
///
/// This is the q3-to-q7 ordering contract exercised by TPC-H. The first plan
/// synchronously loads `c_mktsegment` and asynchronously requests the customer
/// handle while costing its physical access. The peer's plan is the next
/// client statement over the same catalog, so its q7 estimate must use the
/// newly resident handle statistics.
#[test]
fn physical_statistics_loads_become_shared_at_the_next_statement_boundary() {
    std::thread::Builder::new()
        .name("statistics-load-statement-boundary".to_owned())
        // The workspace's configured thread stack (`.cargo/config.toml`'s
        // RUST_MIN_STACK, what node workers run on): a store session's
        // statement runs the node's frames on this thread.
        .stack_size(32 * 1024 * 1024)
        .spawn(|| {
            let mut session = Session::new();
            session
                .run(
                    "CREATE TABLE customer (\
                 c_custkey INT PRIMARY KEY, \
                 c_nationkey INT, \
                 c_mktsegment VARCHAR(10))",
                )
                .unwrap();
            session
                .run("CREATE TABLE orders (o_orderkey INT PRIMARY KEY, o_custkey INT)")
                .unwrap();
            session
                .run("CREATE TABLE supplier (s_suppkey INT PRIMARY KEY, s_nationkey INT)")
                .unwrap();
            session
                .run("CREATE TABLE lineitem (l_suppkey INT, l_orderkey INT, l_shipdate DATE)")
                .unwrap();
            session
                .run("CREATE TABLE nation (n_nationkey INT PRIMARY KEY, n_name VARCHAR(25))")
                .unwrap();
            session
                .run("INSERT INTO customer VALUES (1,1,'AUTOMOBILE'),(2,2,'BUILDING')")
                .unwrap();
            session
                .run("INSERT INTO orders VALUES (1,1),(2,2)")
                .unwrap();
            session
                .run("INSERT INTO supplier VALUES (1,1),(2,2)")
                .unwrap();
            session
                .run(
                    "INSERT INTO lineitem VALUES \
             (1,1,'1995-01-01'),(2,2,'1995-06-01'),\
             (1,1,'1996-01-01'),(2,2,'1996-06-01')",
                )
                .unwrap();
            session
                .run("INSERT INTO nation VALUES (1,'JAPAN'),(2,'INDIA')")
                .unwrap();

            let ctx = session.statement_context(false);
            // The sampled distributions are read from the committed rows
            // (`with_catalog_mut` binds a snapshot), scaled, and installed
            // in the node's statistics cache as Go's tests install theirs.
            let mut published = Vec::new();
            session
                .with_committed_catalog_mut(|catalog| {
            for (table_name, row_count, requested_ndvs) in [
                (
                    "customer",
                    150_000,
                    &[
                        ("c_custkey", 149_568),
                        ("c_nationkey", 25),
                        ("c_mktsegment", 5),
                    ][..],
                ),
                (
                    "orders",
                    1_500_000,
                    &[("o_orderkey", 1_487_616), ("o_custkey", 99_248)][..],
                ),
                (
                    "supplier",
                    10_000,
                    &[("s_suppkey", 10_000), ("s_nationkey", 25)][..],
                ),
                (
                    "lineitem",
                    6_001_215,
                    &[
                        ("l_suppkey", 10_000),
                        ("l_orderkey", 1_487_616),
                        ("l_shipdate", 2_526),
                    ][..],
                ),
                ("nation", 25, &[("n_nationkey", 25), ("n_name", 25)][..]),
            ] {
                let (table_id, column_ndvs, indexes, mut statistics) = {
                    let tidb_infoschema::TableEntry::Kv(table) =
                        catalog.table_mut_in("test", table_name).unwrap()
                    else {
                        panic!("{table_name} is not a KV table");
                    };
                    let table_id = table.table_id();
                    let requested = requested_ndvs
                        .iter()
                        .copied()
                        .collect::<std::collections::BTreeMap<_, _>>();
                    let column_ndvs = table
                        .visible_columns()
                        .into_iter()
                        .map(|(_, column)| (column.id, requested[column.name.as_str()]))
                        .collect::<std::collections::BTreeMap<_, _>>();
                    let indexes = table
                        .indexes()
                        .iter()
                        .map(|index| {
                            let columns = index
                                .column_offsets
                                .iter()
                                .filter_map(|offset| table.columns().get(*offset))
                                .map(|column| column.id)
                                .collect::<Vec<_>>();
                            (index.id, columns)
                        })
                        .collect::<Vec<_>>();
                    let mut options = tidb_executor::analyze::AnalyzeOptions::default();
                    options.num_topn = 0;
                    // This fixture scales every sampled bucket into a fixed
                    // distribution; do not randomly omit its seed rows.
                    options.sample_rate = Some(1.0);
                    let statistics = tidb_executor::analyze::kv::analyze_kv_table(
                        std::sync::Arc::make_mut(table),
                        &options,
                        None,
                        &ctx,
                    )
                    .unwrap();
                    (table_id, column_ndvs, indexes, statistics)
                };

                macro_rules! scale_histogram {
                    ($histogram:expr, $ndv:expr) => {{
                        let histogram = $histogram;
                        let original_non_null = histogram.not_null_count().max(1.0);
                        let bucket_count = histogram.buckets.len() as i64;
                        let bounded_ndv = $ndv.max(1).min(row_count);
                        let mut previous_count = 0;
                        for (position, bucket) in histogram.buckets.iter_mut().enumerate() {
                            let count = if position + 1 == bucket_count as usize {
                                row_count
                            } else {
                                ((bucket.count as f64 / original_non_null) * row_count as f64)
                                    .round() as i64
                            }
                            .max(previous_count)
                            .min(row_count);
                            let rows_in_bucket = count - previous_count;
                            bucket.count = count;
                            bucket.repeat = (row_count / bounded_ndv).max(1).min(rows_in_bucket);
                            bucket.ndv = bounded_ndv / bucket_count
                                + i64::from((position as i64) < bounded_ndv % bucket_count);
                            previous_count = count;
                        }
                        histogram.ndv = bounded_ndv;
                        histogram.null_count = 0;
                    }};
                }

                        for column in statistics.hist_coll.stable_columns() {
                            let mut column = column.write().unwrap();
                            let column_id = column.histogram.id;
                            scale_histogram!(&mut column.histogram, column_ndvs[&column_id]);
                        }
                        if table_name == "customer" {
                            let columns = statistics.hist_coll.stable_columns();
                            let mut column = columns
                                .iter()
                                .map(|column| column.write().unwrap())
                                .find(|column| column.histogram.ndv == 5)
                                .expect("customer.c_mktsegment histogram");
                            column
                                .histogram
                                .buckets
                                .last_mut()
                                .expect("customer.c_mktsegment histogram")
                                .count = 149_998;
                        }
                        for (index_id, index_columns) in indexes {
                            let index_ndv = index_columns
                                .iter()
                                .try_fold(1_i64, |ndv, column_id| {
                                    ndv.checked_mul(column_ndvs[column_id])
                                })
                                .unwrap_or(row_count)
                                .min(row_count)
                                .max(1);
                            let index = statistics.hist_coll.get_index(index_id).unwrap();
                            scale_histogram!(&mut index.write().unwrap().histogram, index_ndv);
                        }
                        statistics.hist_coll.realtime_count = row_count;
                        published.push((table_id, statistics));
                    }
                    Ok(())
                })
                .unwrap();
            let shared = session.shared_catalog();
            for (table_id, statistics) in &published {
                session.publish_table_statistics_for_test(
                    &shared.lock().unwrap(),
                    *table_id,
                    statistics,
                );
            }

            let q3 = "EXPLAIN SELECT * FROM customer, orders \
        WHERE c_mktsegment = 'AUTOMOBILE' AND c_custkey = o_custkey";
            assert!(!row_text(session.run(q3)).is_empty());
            let mut peer = Session::with_catalog(session.shared_catalog());
            let q7 = "EXPLAIN SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue \
        FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation, \
                     EXTRACT(YEAR FROM l_shipdate) AS l_year, 1 AS volume \
              FROM supplier, lineitem, orders, customer, nation n1, nation n2 \
              WHERE s_suppkey = l_suppkey \
                AND o_orderkey = l_orderkey \
                AND c_custkey = o_custkey \
                AND s_nationkey = n1.n_nationkey \
                AND c_nationkey = n2.n_nationkey \
                AND ((n1.n_name = 'JAPAN' AND n2.n_name = 'INDIA') \
                  OR (n1.n_name = 'INDIA' AND n2.n_name = 'JAPAN')) \
                AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31') shipping \
        GROUP BY supp_nation, cust_nation, l_year \
        ORDER BY supp_nation, cust_nation, l_year";
            let rows = row_text(peer.run(q7));
            let final_nation_join = rows
                .iter()
                .find(|row| row[0].contains("HashJoin") && row[4].contains("customer.c_nationkey"))
                .unwrap_or_else(|| panic!("no final nation join in q7 plan: {rows:#?}"));
            assert_eq!(
                final_nation_join[1], "38839.37",
                "the next connection did not observe q3's physical handle request and its \
                 first same-version resident column"
            );
        })
        .unwrap()
        .join()
        .unwrap();
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

#[test]
fn analyze_expression_index_samples_keep_physical_column_positions() {
    for (analyze, columns) in [
        (
            "ANALYZE TABLE expr_samples ALL COLUMNS",
            names(&["a", "unused"]),
        ),
        ("ANALYZE TABLE expr_samples COLUMNS a", names(&["a"])),
    ] {
        let mut session = Session::new();
        session.run("CREATE TABLE expr_samples(a INT, unused INT, v INT AS (a+1), KEY iv(v), KEY ie(a,(a+2)))").unwrap();
        session
            .run("INSERT INTO expr_samples(a,unused) VALUES(1,10),(2,20),(2,30),(3,40)")
            .unwrap();
        session.run(analyze).unwrap();
        // No histogram for the virtual column `v`; both indexes sample the
        // values their expressions take (testkit probe).
        assert_eq!(
            histogram_columns(&mut session, "expr_samples"),
            columns,
            "{analyze}"
        );
        let indexes = stats_histograms(&mut session, "expr_samples")
            .into_iter()
            .filter(|row| row[2] == "1")
            .map(|row| (row[1].clone(), row[3].clone(), row[4].clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            indexes,
            [
                ("iv".to_owned(), "3".to_owned(), "0".to_owned()),
                ("ie".to_owned(), "3".to_owned(), "0".to_owned())
            ],
            "{analyze}"
        );
        let topn = row_text(
            session.run("SHOW STATS_TOPN WHERE table_name = 'expr_samples' AND is_index = 1"),
        )
        .into_iter()
        .map(|row| (row[3].clone(), row[5].clone(), row[6].clone()))
        .collect::<Vec<_>>();
        let expected = [
            ("iv", "2", "1"),
            ("iv", "3", "2"),
            ("iv", "4", "1"),
            ("ie", "(1, 3)", "1"),
            ("ie", "(2, 4)", "2"),
            ("ie", "(3, 5)", "1"),
        ]
        .map(|(index, value, count)| (index.to_owned(), value.to_owned(), count.to_owned()));
        assert_eq!(topn, expected, "{analyze}");
    }
}

#[test]
fn partial_analyze_replaces_unanalyzed_table_metadata() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE partial_metadata(a INT,b INT,KEY ia(a))")
        .unwrap();
    session
        .run("INSERT INTO partial_metadata VALUES(1,10),(2,20),(2,30)")
        .unwrap();
    session.run("FLUSH STATS_DELTA *.*").unwrap();
    session
        .run("ANALYZE TABLE partial_metadata COLUMNS a")
        .unwrap();
    let shared = session.shared_catalog();
    let catalog = shared.lock().unwrap();
    let TableEntry::Kv(table) = catalog.table_in("test", "partial_metadata").unwrap() else {
        panic!("KV table")
    };
    let stats = catalog.table_statistics_in(&shared.stats_handle(), table.table_id()).unwrap();
    assert_eq!(stats.hist_coll.stats_version, 2);
    assert!(!stats.hist_coll.pseudo);
    assert!(stats.is_initialized());
    assert_eq!(stats.hist_coll.realtime_count, 3);
    drop(catalog);
    let plan = row_text(session.run("EXPLAIN SELECT * FROM partial_metadata WHERE a=2"));
    assert!(
        plan.iter()
            .flatten()
            .all(|cell| !cell.contains("stats:pseudo")),
        "{plan:?}"
    );
}

/// Go globalstats retains exact local-unique NDVs but no global FM sketch.
#[test]
fn stats_ndv_batch_unique_partition_lifecycle() {
    let mut session = Session::new();
    session
        .run("CREATE TABLE exact_ndv (a INT, UNIQUE INDEX i(a)) PARTITION BY HASH(a) PARTITIONS 2")
        .unwrap();
    session
        .run("INSERT INTO exact_ndv VALUES (0),(1),(2),(3),(4),(5),(NULL),(NULL)")
        .unwrap();
    session
        .run("ANALYZE TABLE exact_ndv ALL COLUMNS WITH 0 TOPN, 1 SAMPLERATE")
        .unwrap();
    assert!(
        stats_meta_rows(&mut session, "exact_ndv").contains(&("global".to_owned(), "8".to_owned()))
    );
    let global = stats_histograms(&mut session, "exact_ndv")
        .into_iter()
        .filter(|row| row[0] == "global")
        .map(|row| (row[1].clone(), row[3].clone(), row[4].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        global,
        [
            ("a".to_owned(), "6".to_owned(), "2".to_owned()),
            ("i".to_owned(), "6".to_owned(), "2".to_owned())
        ]
    );
    // FM sketches are each partition's; the merged global statistics keep
    // none (testkit probe: `mysql.stats_fm_sketch` rows for p0 and p1 only).
    let sketches = row_text(session.run(
        "SELECT p.partition_name, f.is_index FROM mysql.stats_fm_sketch f \
         LEFT JOIN information_schema.partitions p ON f.table_id = p.tidb_partition_id \
         WHERE f.table_id IN (\
           SELECT tidb_table_id FROM information_schema.tables WHERE table_name = 'exact_ndv' \
           UNION SELECT tidb_partition_id FROM information_schema.partitions \
             WHERE table_name = 'exact_ndv') \
         ORDER BY 1, 2",
    ));
    assert_eq!(
        sketches,
        [["p0", "0"], ["p0", "1"], ["p1", "0"], ["p1", "1"]]
    );
}

/// Go `buildAnalyzeSamplingPushdown`'s note, as a Go server on this branch
/// reports it. A never-analyzed table reads as the 10000-row pseudo table;
/// once analyzed empty, the PD helper (this store has no PD HTTP client)
/// answers its cached miss as `hasPD`, so Go names the empty-table reason
/// without the PD clause. Partitions are the tasks of a partitioned table,
/// and an explicit SAMPLES or SAMPLERATE takes no adjusted rate.
#[test]
fn analyze_notes_the_adjusted_sample_rate_like_go() {
    let note = |target: &str, reason: &str| {
        vec![
            "Note".to_owned(),
            "1105".to_owned(),
            format!(
                "Analyze use auto adjusted sample rate 1.000000 for table {target}, \
                 reason to use this rate is \"{reason}\""
            ),
        ]
    };
    let pseudo = "use min(1, 110000/10000) as the sample-rate=1";
    let empty = "TiDB assumes that the table is empty, use sample-rate=1";
    let mut session = Session::new();
    session.run("CREATE TABLE t (a INT, KEY ka(a))").unwrap();
    session.run("ANALYZE TABLE t").unwrap();
    assert_eq!(row_text(session.run("SHOW WARNINGS")), vec![note("test.t", pseudo)]);
    session.run("ANALYZE TABLE t").unwrap();
    assert_eq!(row_text(session.run("SHOW WARNINGS")), vec![note("test.t", empty)]);

    session
        .run("CREATE TABLE p (a INT, b INT, KEY(a)) PARTITION BY HASH(a) PARTITIONS 2")
        .unwrap();
    session.run("ANALYZE TABLE p").unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![
            note("test.p's partition p0", pseudo),
            note("test.p's partition p1", pseudo)
        ]
    );
    session.run("ANALYZE TABLE p PARTITION p1").unwrap();
    assert_eq!(
        row_text(session.run("SHOW WARNINGS")),
        vec![note("test.p's partition p1", empty)]
    );

    session.run("CREATE TABLE s (a INT)").unwrap();
    session.run("ANALYZE TABLE s WITH 0.5 SAMPLERATE").unwrap();
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
    session.run("ANALYZE TABLE s WITH 100 SAMPLES").unwrap();
    assert!(row_text(session.run("SHOW WARNINGS")).is_empty());
}

/// `ANALYZE` stores what it published as Go's `SaveAnalyzeResultToStorage`
/// writes it -- the `stats_meta` row, each histogram row, the TopN values as
/// their exact key bytes -- and finishes its `analyze_jobs` row, whose start
/// and finish read `@@TIME_ZONE` through the session's variable binding.
#[test]
fn analyze_saves_statistics_and_finishes_its_job_row() {
    let mut session = Session::new();
    session
        .run("create table t (a int, b varchar(10), key ia(a))")
        .unwrap();
    session
        .run("insert into t values (1, 'x'), (2, 'y'), (2, 'z'), (3, null)")
        .unwrap();
    session.run("analyze table t").unwrap();
    let id = "(select tidb_table_id from information_schema.tables \
              where table_schema = 'test' and table_name = 't')";
    // Go's save transaction stamps `version` after the analyze snapshot it
    // records (oracle: `version > snapshot`); the in-process tier writes
    // both from one clock reading.
    let version_vs_snapshot = "version > snapshot";
    assert_eq!(
        row_text(session.run(&format!(
            "select count, modify_count, {version_vs_snapshot} from mysql.stats_meta \
             where table_id = {id}"
        ))),
        [["4", "0", "1"]]
    );
    assert_eq!(
        row_text(session.run(&format!(
            "select is_index, hist_id, distinct_count, null_count, stats_ver, \
             cm_sketch is null from mysql.stats_histograms where table_id = {id} \
             order by is_index, hist_id"
        ))),
        [
            ["0", "1", "3", "0", "2", "1"],
            ["0", "2", "3", "1", "2", "1"],
            ["1", "1", "3", "0", "2", "1"],
        ]
    );
    assert_eq!(
        row_text(session.run(&format!(
            "select is_index, hex(value), count from mysql.stats_top_n \
             where table_id = {id} and hist_id = 1 order by is_index, value"
        ))),
        [
            ["0", "038000000000000001", "1"],
            ["0", "038000000000000002", "2"],
            ["0", "038000000000000003", "1"],
            ["1", "038000000000000001", "1"],
            ["1", "038000000000000002", "2"],
            ["1", "038000000000000003", "1"],
        ]
    );
    assert_eq!(
        row_text(session.run(
            "select table_schema, table_name, state, processed_rows, fail_reason is null, \
             process_id is null, start_time is not null, end_time is not null \
             from mysql.analyze_jobs"
        )),
        [["test", "t", "finished", "4", "1", "1", "1", "1"]]
    );
}

/// `DROP STATS` is Go's soft delete: the meta row keeps its counts, each
/// histogram row is reset in place, the buckets and TopN go, and the cache
/// reloads from what storage now holds -- a table with a real row count
/// whose distribution is pseudo again. The statistics id comes from a
/// `SET` whose subquery reads information_schema.
#[test]
fn drop_stats_soft_deletes_storage_and_reloads_the_table() {
    let mut session = Session::new();
    session
        .run("create table t (c1 varchar(20), c2 varchar(20))")
        .unwrap();
    session
        .run("insert into t values ('1', '1'), ('2', '2'), ('3', '3'), ('4', '4')")
        .unwrap();
    session.run("insert into t select * from t").unwrap();
    session.run("insert into t select * from t").unwrap();
    session
        .run("analyze table t all columns with 2 topn")
        .unwrap();
    session
        .run(
            "set @a = (select tidb_table_id from information_schema.tables \
             where table_name = 't' and table_schema = 'test')",
        )
        .unwrap();
    assert_eq!(
        row_text(session.run("select count(*) > 0 from mysql.stats_top_n where table_id = @a")),
        [["1"]]
    );
    session.run("drop stats t").unwrap();
    assert_eq!(
        row_text(
            session.run("select modify_count, count from mysql.stats_meta where table_id = @a")
        ),
        [["0", "16"]]
    );
    assert_eq!(
        row_text(session.run(
            "select hist_id, distinct_count, stats_ver from mysql.stats_histograms \
             where table_id = @a order by hist_id"
        )),
        [["1", "0", "0"], ["2", "0", "0"]]
    );
    assert!(
        row_text(session.run("select * from mysql.stats_buckets where table_id = @a")).is_empty()
    );
    assert!(
        row_text(session.run("select * from mysql.stats_top_n where table_id = @a")).is_empty()
    );
    let (est_rows, info) = scan_row(&mut session, "explain select * from t");
    assert_eq!(est_rows, "16.00");
    assert!(info.contains("stats:pseudo"), "{info}");
    assert!(session
        .run("drop stats nosuch")
        .unwrap_err()
        .to_string()
        .contains("doesn't exist"));
}

/// Go `executeFlushStatsDeltaOnCurrentInstance` warns for each named object
/// it cannot find and flushes the rest (captured from Go on this branch).
#[test]
fn flush_stats_delta_warns_for_missing_objects_like_go() {
    let mut session = Session::new();
    session.run("create table t1 (x int)").unwrap();
    session.run("flush stats_delta test.nosuch").unwrap();
    assert_eq!(
        row_text(session.run("show warnings")),
        [["Warning", "1146", "Table 'test.nosuch' doesn't exist"]]
    );
    session.run("flush stats_delta nosuchdb.*").unwrap();
    assert_eq!(
        row_text(session.run("show warnings")),
        [["Warning", "1049", "Unknown database 'nosuchdb'"]]
    );
    session
        .run("flush stats_delta test.nosuch, test.t1")
        .unwrap();
    assert_eq!(
        row_text(session.run("show warnings")),
        [["Warning", "1146", "Table 'test.nosuch' doesn't exist"]]
    );
}

/// Go `GetPhysicalIDsAndPartitionNames` matches a partition by its Unicode
/// lower-case name (`def.Name.L == name.L`): `pä` and `PÄ` both name `pÄ`.
#[test]
fn drop_stats_matches_partition_names_by_unicode_lower_case() {
    let mut session = Session::new();
    session
        .run(
            "create table pt (a int) partition by range (a) \
             (partition pÄ values less than (10), partition p1 values less than (20))",
        )
        .unwrap();
    for name in ["pä", "PÄ"] {
        session
            .run(&format!("drop stats pt partition {name}"))
            .unwrap();
        assert_eq!(
            row_text(session.run("show warnings")),
            [[
                "Warning",
                "1681",
                "'DROP STATS ... PARTITION ...' is deprecated and will be removed in a future release."
            ]]
        );
    }
}
