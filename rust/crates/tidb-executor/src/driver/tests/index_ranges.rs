//! Index range scans: the intervals a predicate becomes, and the rows that
//! reading those intervals returns.
//!
//! Includes the equivalence check that a multi-column, multi-range scan reads
//! exactly the rows a full scan would, and the direct comparison of built
//! ranges against the ones Go's builder produces. Mirrors Go
//! `pkg/util/ranger` feeding `pkg/executor`'s index reader.

use super::*;

/// A composite-index range spans several datums per bound, an IN list
/// produces several ranges, and an OR unions them. The answers must be
/// the same rows a full scan would return -- a range that reads too few
/// rows is invisible to the range text alone.
#[test]
fn multi_column_and_multi_range_scans_read_the_same_rows_as_a_full_scan() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE m (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, KEY ab (a, b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO m VALUES (1, 1, 1), (2, 1, 5), (3, 1, 9), (4, 2, 5), \
         (5, 3, 5), (6, NULL, 1), (7, 2, NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let ids = |sql: &str| {
        let mut ids: Vec<i64> = run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(v) => v,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        ids
    };

    // Equality on the leading column plus a range on the next.
    assert_eq!(ids("SELECT id FROM m WHERE a = 1 AND b > 1"), vec![2, 3]);
    assert_eq!(
        ids("SELECT id FROM m WHERE a = 1 AND b BETWEEN 1 AND 5"),
        vec![1, 2]
    );
    // An IN list on the leading column: several point ranges, each
    // extended by the equality on the next column.
    assert_eq!(
        ids("SELECT id FROM m WHERE a IN (1, 3) AND b = 5"),
        vec![2, 5]
    );
    // A disjunction: the branches' ranges are unioned.
    assert_eq!(
        ids("SELECT id FROM m WHERE (a = 1 AND b = 5) OR (a = 3 AND b = 5)"),
        vec![2, 5]
    );
    // A NULL in the indexed columns is reachable only through IS NULL,
    // never through a comparison.
    assert_eq!(ids("SELECT id FROM m WHERE a IS NULL"), vec![6]);
    assert_eq!(ids("SELECT id FROM m WHERE a = 2 AND b IS NULL"), vec![7]);
    // The residual half still filters: `id` is not in the index, so the
    // range cannot express it and the Selection above must.
    assert_eq!(ids("SELECT id FROM m WHERE a = 1 AND id > 1"), vec![2, 3]);
}

/// Index range scans: a comparison on an indexed column reads the rows the
/// index covers instead of scanning the table, with Go's range semantics.
#[test]
fn index_range_scans() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE r (id BIGINT PRIMARY KEY, score BIGINT, KEY score_idx (score))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO r VALUES (1, 10), (2, 20), (3, 30), (4, 20), (5, NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    let ids = |sql: &str, catalog: &Catalog| {
        let mut got: Vec<i64> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(value) => value,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect();
        got.sort_unstable();
        got
    };

    assert_eq!(
        ids("SELECT id FROM r WHERE score > 15", &catalog),
        vec![2, 3, 4]
    );
    assert_eq!(
        ids("SELECT id FROM r WHERE score >= 20", &catalog),
        vec![2, 3, 4]
    );
    assert_eq!(
        ids("SELECT id FROM r WHERE score < 30", &catalog),
        vec![1, 2, 4]
    );
    assert_eq!(ids("SELECT id FROM r WHERE score <= 10", &catalog), vec![1]);
    assert_eq!(
        ids("SELECT id FROM r WHERE score = 20", &catalog),
        vec![2, 4]
    );
    // The constant may sit on the left, with the operator flipped.
    assert_eq!(
        ids("SELECT id FROM r WHERE 15 < score", &catalog),
        vec![2, 3, 4]
    );

    // Several conditions on the column intersect into one range.
    assert_eq!(
        ids("SELECT id FROM r WHERE score > 10 AND score < 30", &catalog),
        vec![2, 4]
    );
    assert_eq!(
        ids(
            "SELECT id FROM r WHERE score >= 20 AND score <= 20",
            &catalog
        ),
        vec![2, 4]
    );

    // Go's ranges start at MinNotNull, so a NULL satisfies no comparison
    // -- row 5 never appears, and IS NULL still finds it through the scan.
    assert_eq!(
        ids("SELECT id FROM r WHERE score > -100", &catalog),
        vec![1, 2, 3, 4]
    );
    assert_eq!(
        ids("SELECT id FROM r WHERE score IS NULL", &catalog),
        vec![5]
    );

    // A condition the ranges do not consume still filters, because the
    // WHERE stays above the read.
    assert_eq!(
        ids("SELECT id FROM r WHERE score > 15 AND id = 3", &catalog),
        vec![3]
    );

    // Writes are visible to a later range scan, including through the
    // index entries a DELETE removed.
    run_update_on(
        "UPDATE r SET score = 99 WHERE id = 1",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(ids("SELECT id FROM r WHERE score > 50", &catalog), vec![1]);
    run_delete_on(
        "DELETE FROM r WHERE id = 1",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        ids("SELECT id FROM r WHERE score > 50", &catalog),
        Vec::<i64>::new()
    );
}

#[test]
fn use_index_merge_unions_indexed_or_branches_without_duplicate_rows() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE im (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, KEY ia (a), KEY ib (b))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO im VALUES (1, 1, 2), (2, 1, 0), (3, 0, 2), (4, 0, 0), (5, 1, 9)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT /*+ USE_INDEX_MERGE(im, ia, ib) */ id FROM im WHERE a = 1 OR b = 2";
    let mut ids: Vec<i64> = run_select_on(sql, &catalog, &ctx)
        .unwrap()
        .into_iter()
        .map(|row| match row[0] {
            Datum::Int(value) => value,
            ref other => panic!("expected an integer handle, got {other:?}"),
        })
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![1, 2, 3, 5]);

    let mut residual_ids: Vec<i64> = run_select_on(
        "SELECT /*+ USE_INDEX_MERGE(im, ia, ib) */ id FROM im WHERE (a = 1 AND b = 0) OR b = 2",
        &catalog,
        &ctx,
    )
    .unwrap()
    .into_iter()
    .map(|row| match row[0] {
        Datum::Int(value) => value,
        ref other => panic!("expected an integer handle, got {other:?}"),
    })
    .collect();
    residual_ids.sort_unstable();
    assert_eq!(residual_ids, vec![1, 2, 3]);

    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    let plan: Vec<String> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|datum| match datum {
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect();
    assert!(
        plan.iter().any(|line| line.contains("IndexMerge")),
        "USE_INDEX_MERGE must install the multi-index reader, got {plan:?}"
    );
}

#[test]
fn use_index_merge_intersects_indexed_and_branches() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE imi (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, KEY ia (a), KEY ib (b))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO imi VALUES (1, 1, 2), (2, 1, 0), (3, 0, 2), (4, 0, 0)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT /*+ USE_INDEX_MERGE(imi, ia, ib) */ id FROM imi WHERE a = 1 AND b = 2";
    assert_eq!(
        run_select_on(sql, &catalog, &ctx).unwrap(),
        vec![vec![Datum::Int(1)]]
    );

    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    let plan: Vec<String> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|datum| match datum {
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join(" | ")
        })
        .collect();
    assert!(
        plan.iter()
            .any(|line| line.contains("IndexMerge") && line.contains("intersection")),
        "USE_INDEX_MERGE must install the intersection reader, got {plan:?}"
    );
}

#[test]
fn no_index_merge_overrides_a_use_index_merge_hint() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE imn (id BIGINT PRIMARY KEY, a BIGINT, b BIGINT, KEY ia (a), KEY ib (b))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on(
        "INSERT INTO imn VALUES (1, 1, 2), (2, 1, 0), (3, 0, 2)",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let sql = "SELECT /*+ USE_INDEX_MERGE(imn, ia, ib) NO_INDEX_MERGE() */ id FROM imn WHERE a = 1 OR b = 2";
    let stmt = tidb_parser::parse(sql).unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query");
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a SELECT");
    };
    let (_, rows) =
        explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Row).unwrap();
    assert!(
        rows.iter().all(|row| match &row[0] {
            Datum::Bytes(id) => !String::from_utf8_lossy(id).contains("IndexMerge"),
            _ => true,
        }),
        "NO_INDEX_MERGE must suppress the reader, got {rows:?}"
    );
}

/// A range scan over a UNIQUE index reads its handles out of the entry
/// VALUES, not the key, so this covers the other half of the entry format
/// -- including the NULL entries a unique index stores non-distinctly.
#[test]
fn index_range_scan_over_a_unique_index() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE u2 (id BIGINT PRIMARY KEY, code BIGINT UNIQUE)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u2 VALUES (1, 100), (2, 200), (3, 300), (4, NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let mut ids: Vec<Datum> = run_select_on(
        "SELECT id FROM u2 WHERE code >= 200",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap()
    .into_iter()
    .map(|row| row[0].clone())
    .collect();
    ids.sort_by_key(|value| match value {
        Datum::Int(v) => *v,
        other => panic!("expected an int, got {other:?}"),
    });
    assert_eq!(ids, vec![Datum::Int(2), Datum::Int(3)]);
    // The NULL row is reachable, just never through a comparison.
    assert_eq!(
        run_select_on(
            "SELECT id FROM u2 WHERE code IS NULL",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(4)]]
    );
}

/// The answers above would be right even from a full scan, so this asserts
/// the DECISION and the ranges themselves.
#[test]
fn index_ranges_are_built_the_way_go_builds_them() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE q (id BIGINT PRIMARY KEY, score BIGINT, note VARCHAR(8), KEY s (score))",
        &mut catalog,
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("q") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.field_type.clone()))
        .collect::<Vec<_>>();
    let ranges = |sql: &str| {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query")
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a select")
        };
        let scope = crate::plan_trace::PlanTrace::single_table_scope("q", None, columns.clone());
        // This case is about INDEX paths; a table path chosen here would be a
        // different assertion and must not be silently read as one.
        match choose_index_range_path(
            select,
            &catalog,
            &scope,
            table,
            &columns,
            &crate::index_hints::AvailablePaths::unrestricted(),
            false,
        ) {
            Some(crate::driver::access::ChosenPath::Index(id, ranges, _, _)) => Some((id, ranges)),
            Some(crate::driver::access::ChosenPath::HandleRange(ranges, _)) => {
                panic!("expected an index path, got a handle range {ranges:?}")
            }
            None => None,
        }
    };

    // Go: GT is (v, MaxValue], LT is [MinNotNull, v).
    assert_eq!(
        ranges("SELECT id FROM q WHERE score > 5"),
        Some((
            1,
            vec![IndexRange {
                low: vec![Datum::Int(5)],
                high: vec![Datum::MaxValue],
                low_exclusive: true,
                high_exclusive: false,
            }]
        ))
    );
    assert_eq!(
        ranges("SELECT id FROM q WHERE score < 5"),
        Some((
            1,
            vec![IndexRange {
                low: vec![Datum::MinNotNull],
                high: vec![Datum::Int(5)],
                low_exclusive: false,
                high_exclusive: true,
            }]
        ))
    );
    // An intersection keeps the tighter end of each side.
    assert_eq!(
        ranges("SELECT id FROM q WHERE score > 5 AND score <= 9"),
        Some((
            1,
            vec![IndexRange {
                low: vec![Datum::Int(5)],
                high: vec![Datum::Int(9)],
                low_exclusive: true,
                high_exclusive: false,
            }]
        ))
    );
    // A NULL constant matches nothing, which Go represents as no ranges.
    assert_eq!(
        ranges("SELECT id FROM q WHERE score > NULL"),
        Some((1, vec![]))
    );

    // An OR is detached branch by branch and the branches' ranges are
    // unioned (Go `detachDNFCondAndBuildRangeForIndex` + `UnionRanges`).
    assert_eq!(
        ranges("SELECT id FROM q WHERE score > 1 OR score < 0"),
        Some((
            1,
            vec![
                IndexRange {
                    low: vec![Datum::MinNotNull],
                    high: vec![Datum::Int(0)],
                    low_exclusive: false,
                    high_exclusive: true,
                },
                IndexRange {
                    low: vec![Datum::Int(1)],
                    high: vec![Datum::MaxValue],
                    low_exclusive: true,
                    high_exclusive: false,
                }
            ]
        ))
    );

    // No usable index: the `WHERE` names an unindexed column that `s` does
    // not store either, so no index path can answer the statement alone and
    // the table scan stands. Captured (v8.5 `gorun`):
    //
    // ```text
    // explain SELECT id FROM q WHERE note = 'x'
    //   TableFullScan_9  cop[tikv]  table:q  keep order:false, stats:pseudo
    // ```
    assert_eq!(ranges("SELECT id FROM q WHERE note = 'x'"), None);
    // No `WHERE` at all is not the same thing: `s(score)` carries the integer
    // handle, so it COVERS `SELECT id` and Go reads the whole of it rather
    // than the whole table (`skylinePruning`'s `path.IsSingleScan`). Captured:
    //
    // ```text
    // explain SELECT id FROM q
    //   IndexFullScan_6  cop[tikv]  table:q, index:s(score)  keep order:false, stats:pseudo
    // ```
    assert_eq!(
        ranges("SELECT id FROM q"),
        Some((1, vec![IndexRange::full()]))
    );
}

/// THE ROW-LEVEL PROOF that a `LIKE` bound is a weight string.
///
/// A range built by incrementing the RAW low bytes and then collating it is
/// not the range Go builds, and on a case-insensitive collation the
/// difference is not a wider scan but a MISSING one: the derived upper bound
/// can collate to the lower bound (or below it), the range empties, and the
/// statement answers nothing. Range text alone cannot show that -- an empty
/// range prints as no scan at all -- so this asserts the ROWS.
///
/// Every expectation was captured from real TiDB (`gorun`) over
/// `ci(a varchar(50) collate utf8mb4_general_ci, key idx(a))` holding
/// ``'ab`q', 'abAq', 'abzz', 'abcq'``.
#[test]
fn a_like_over_a_case_insensitive_index_returns_gos_rows() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE ci (a VARCHAR(50) COLLATE utf8mb4_general_ci, KEY idx (a))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO ci VALUES ('ab`q'), ('abAq'), ('abzz'), ('abcq'), ('abéxx')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let rows = |sql: &str| {
        let mut out: Vec<String> = run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .iter()
            .map(|row| datum_text_for_test(&row[0]))
            .collect();
        out.sort();
        out
    };
    // `é` and `ê` share a weight, so the raw increment collated to exactly
    // the lower bound and this answered NOTHING.
    assert_eq!(rows("SELECT a FROM ci WHERE a LIKE 'abé%'"), ["abéxx"]);
    // The same failure in pure ASCII: '`'+1 is 'a', whose weight is 'A''s.
    assert_eq!(rows("SELECT a FROM ci WHERE a LIKE 'ab`%'"), ["ab`q"]);
    // The pattern's own case must not matter -- both of these read the one
    // row spelled `'abAq'`, which is what makes the bound a WEIGHT string
    // rather than a text one.
    assert_eq!(rows("SELECT a FROM ci WHERE a LIKE 'abA%'"), ["abAq"]);
    assert_eq!(rows("SELECT a FROM ci WHERE a LIKE 'aba%'"), ["abAq"]);
    // The direction that was merely too wide, kept as a control.
    assert_eq!(rows("SELECT a FROM ci WHERE a LIKE 'abz%'"), ["abzz"]);
}

/// The PAD SPACE half: the stored index key has its trailing spaces trimmed,
/// so a `LIKE` whose literal prefix ENDS in spaces must start its scan at the
/// trimmed key or it steps straight past the entries it wants.
///
/// Captured from real TiDB over `bn(a varchar(50) collate utf8mb4_bin, key
/// idx(a))`: `range:["abc","abc !")`, and
/// `select concat('[',a,']') from bn where a like 'abc  %'` returns
/// `[abc  ]` and `[abc  x]`.
#[test]
fn a_like_with_trailing_spaces_starts_at_the_trimmed_key() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE bn (a VARCHAR(50) COLLATE utf8mb4_bin, KEY idx (a))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO bn VALUES ('abc  '), ('abc  x'), ('abc'), ('abd')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let mut got: Vec<String> = run_select_on(
        "SELECT a FROM bn WHERE a LIKE 'abc  %'",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap()
    .iter()
    .map(|row| datum_text_for_test(&row[0]))
    .collect();
    got.sort();
    assert_eq!(got, ["abc  ", "abc  x"]);
}

/// The clustered integer handle is a trailing key part of every non-unique
/// secondary index, so the ranger narrows on it -- Go `fillIndexPath`
/// (`pkg/planner/core/stats.go`).
///
/// Captured from real TiDB on `explain_easy`'s own schema, which is where the
/// recording's divergence lived:
///
/// ```text
/// create table t1 (c1 int primary key, c2 int, c3 int, index c2 (c2));
/// explain select * from t1 where c1 > 1 and c2 = 1 and c3 < 1;
///   IndexRangeScan_8(Build) | index:c2(c2) | range:(1 1,1 +inf]
/// ```
///
/// and on the sysbench schema, with the index forced because TiDB costs the
/// table path cheaper there:
///
/// ```text
/// explain select c from sbtest1 use index(k_1) where id > 0 and k = 4
///   IndexRangeScan_6(Build) | index:k_1(k) | range:(4 0,4 +inf]
/// ```
#[test]
fn a_non_unique_index_ranges_on_the_clustered_handle_too() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE t1 (c1 INT PRIMARY KEY, c2 INT, c3 INT, INDEX c2 (c2))",
        &mut catalog,
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("t1") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.field_type.clone()))
        .collect::<Vec<_>>();
    let ranges = |sql: &str| {
        let stmt = tidb_parser::parse(sql).unwrap();
        let Stmt::Query(query) = &stmt else {
            panic!("not a query")
        };
        let QueryStmt::Select(select) = &**query else {
            panic!("not a select")
        };
        let scope = crate::plan_trace::PlanTrace::single_table_scope("t1", None, columns.clone());
        match choose_index_range_path(
            select,
            &catalog,
            &scope,
            table,
            &columns,
            &crate::index_hints::AvailablePaths::unrestricted(),
            false,
        ) {
            Some(crate::driver::access::ChosenPath::Index(_, ranges, _, _)) => Some(ranges),
            Some(crate::driver::access::ChosenPath::HandleRange(ranges, _)) => {
                panic!("expected an index path, got a handle range {ranges:?}")
            }
            None => panic!("expected an index path, got the whole-table scan"),
        }
    };

    // `(1 1,1 +inf]`: the point on the declared key part, the handle's range
    // appended behind it.
    assert_eq!(
        ranges("SELECT * FROM t1 WHERE c1 > 1 AND c2 = 1 AND c3 < 1"),
        Some(vec![IndexRange {
            low: vec![Datum::Int(1), Datum::Int(1)],
            high: vec![Datum::Int(1), Datum::MaxValue],
            low_exclusive: true,
            high_exclusive: false,
        }])
    );
    // With nothing said about the handle the range stops at the declared key
    // part, exactly as it did before the handle was a key part at all.
    assert_eq!(
        ranges("SELECT * FROM t1 WHERE c2 = 1"),
        Some(vec![IndexRange {
            low: vec![Datum::Int(1)],
            high: vec![Datum::Int(1)],
            low_exclusive: false,
            high_exclusive: false,
        }])
    );
}

/// The ROW SET, on data where a wrong second key part loses or invents rows.
///
/// The handle range is the only part of this that can go wrong silently: a
/// range text is checked above, but a range whose SECOND dimension is encoded
/// against the wrong bytes reads a different, still non-empty, set of index
/// entries. Every predicate here is answered twice -- once through the index
/// path and once through a table path the same statement is forced onto -- and
/// the two must agree row for row.
///
/// The data is chosen so that agreement is not cheap: NEGATIVE and zero
/// handles (the signed encoding), one `c2` value spread across many handles
/// (so the second dimension actually cuts), a NULL `c2` (an index entry the
/// handle range must not reach), and a handle whose value collides with a
/// `c2` value (so a dimension read in the wrong order would still find rows).
#[test]
fn a_handle_range_reads_the_rows_a_full_scan_reads() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE h (c1 BIGINT PRIMARY KEY, c2 BIGINT, c3 BIGINT, INDEX c2 (c2))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO h VALUES (-9, 1, 0), (-1, 1, 0), (0, 1, 0), (1, 1, 0), (2, 1, 0), \
         (3, 1, 0), (4, 2, 0), (5, NULL, 0), (6, -1, 0), (7, 0, 0)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let ids = |sql: &str| {
        let mut ids: Vec<i64> = run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(v) => v,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        ids
    };
    for predicate in [
        "c2 = 1 AND c1 > 1",
        "c2 = 1 AND c1 >= 1",
        "c2 = 1 AND c1 < 0",
        "c2 = 1 AND c1 <= -1",
        "c2 = 1 AND c1 BETWEEN -1 AND 2",
        "c2 = 1 AND c1 > -100",
        "c2 = 1 AND c1 <> 0",
        "c2 IN (1, 2) AND c1 > 0",
        "c2 = -1 AND c1 > 0",
        "c2 = 0 AND c1 > 0",
        "c2 IS NULL AND c1 > 0",
        "c2 = 1 AND c1 > 3",
        "c2 = 1 AND c1 > 100",
    ] {
        assert_eq!(
            ids(&format!(
                "SELECT c1 FROM h USE INDEX (c2) WHERE {predicate}"
            )),
            ids(&format!(
                "SELECT c1 FROM h IGNORE INDEX (c2) WHERE {predicate}"
            )),
            "index and table paths disagree on `{predicate}`"
        );
    }
    // The absolute answers, so that "both paths wrong the same way" is not a
    // pass: `c2 = 1` holds for handles -9, -1, 0, 1, 2, 3.
    assert_eq!(
        ids("SELECT c1 FROM h USE INDEX (c2) WHERE c2 = 1 AND c1 > 1"),
        vec![2, 3]
    );
    assert_eq!(
        ids("SELECT c1 FROM h USE INDEX (c2) WHERE c2 = 1 AND c1 <= -1"),
        vec![-9, -1]
    );
    assert_eq!(
        ids("SELECT c1 FROM h USE INDEX (c2) WHERE c2 = 1 AND c1 BETWEEN -1 AND 2"),
        vec![-1, 0, 1, 2]
    );
    assert_eq!(
        ids("SELECT c1 FROM h USE INDEX (c2) WHERE c2 IS NULL AND c1 > 0"),
        vec![5]
    );
}

/// A UNIQUE index gets no handle appended, because a DISTINCT entry does not
/// carry the handle in its KEY -- it lives in the value, where no range can
/// reach it. Go states the same condition as `!path.Index.Unique`.
#[test]
fn a_unique_index_gets_no_handle_dimension() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE u (c1 BIGINT PRIMARY KEY, c2 BIGINT, UNIQUE KEY uc2 (c2))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES (1, 10), (2, 20), (3, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("u") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.field_type.clone()))
        .collect::<Vec<_>>();
    let stmt =
        tidb_parser::parse("SELECT * FROM u USE INDEX (uc2) WHERE c2 = 20 AND c1 > 1").unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query")
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a select")
    };
    let scope = crate::plan_trace::PlanTrace::single_table_scope("u", None, columns.clone());
    if let Some(crate::driver::access::ChosenPath::Index(_, ranges, _, _)) = choose_index_range_path(
        select,
        &catalog,
        &scope,
        table,
        &columns,
        &crate::index_hints::AvailablePaths::unrestricted(),
        false,
    ) {
        assert!(
            ranges.iter().all(|range| range.low.len() == 1),
            "a unique index must not range on the handle: {ranges:?}"
        );
    }
    // And the rows are still right either way.
    let rows = run_select_on(
        "SELECT c1 FROM u USE INDEX (uc2) WHERE c2 = 20 AND c1 > 1",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], Datum::Int(2));
}

/// An UNSIGNED clustered handle gets no handle dimension, and the rows prove
/// why: `KvTable::index_key` appends the handle as a SIGNED `Datum::Int`
/// whatever the column declares, so `18446744073709551615` is stored behind
/// the key bytes of `-1` and sorts BELOW `1`. A range of `(1, +inf]` over the
/// column's own unsigned type would read past it and LOSE the row.
///
/// Go refuses the append for the same reason, spelled as
/// `!mysql.HasUnsignedFlag(handleCol.RetType.GetFlag())`. Captured:
///
/// ```text
/// create table hu (c1 bigint unsigned primary key, c2 int, index c2 (c2));
/// explain select * from hu use index(c2) where c2 = 1 and c1 > 1;
///   IndexRangeScan_5 | index:c2(c2) | range:[1,1]
///   Selection_6      | gt(test.hu.c1, 1)
/// select c1 from hu use index(c2) where c2 = 1 and c1 > 1;
///   9223372036854775807; 9223372036854775808; 18446744073709551615
/// ```
#[test]
fn an_unsigned_clustered_handle_gets_no_handle_dimension() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE hu (c1 BIGINT UNSIGNED PRIMARY KEY, c2 INT, INDEX c2 (c2))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO hu VALUES (1, 1), (9223372036854775807, 1), \
         (9223372036854775808, 1), (18446744073709551615, 1)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let rows = run_select_on(
        "SELECT c1 FROM hu USE INDEX (c2) WHERE c2 = 1 AND c1 > 1",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let mut read: Vec<String> = rows.iter().map(|row| format!("{:?}", row[0])).collect();
    read.sort();
    assert_eq!(read.len(), 3, "an unsigned handle row was lost: {read:?}");
    // The largest handle, the one a signed range bound sorts below zero,
    // has to be among them.
    assert!(
        read.iter()
            .any(|cell| cell.contains("18446744073709551615")),
        "the 2^64-1 handle was lost: {read:?}"
    );
}

/// A handle that is ALREADY a declared key part is not appended a second time
/// -- Go's `alreadyHandle` test, whose comment is "Don't add one column twice
/// to the index. May cause unexpected errors."
///
/// Captured: `KEY c2c1(c2, c1)` over `c1 BIGINT PRIMARY KEY` ranges
/// `(1 1,1 +inf]` -- TWO dimensions, from the two DECLARED columns.
#[test]
fn a_handle_that_is_already_a_key_part_is_not_appended_again() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE hd (c1 BIGINT PRIMARY KEY, c2 INT, INDEX c2c1 (c2, c1))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO hd VALUES (1, 1), (2, 1), (3, 1)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let Some(TableEntry::Kv(table)) = catalog.get_table_for_test("hd") else {
        panic!("expected a kv table");
    };
    let columns = table
        .columns
        .iter()
        .map(|c| (c.name.clone(), c.field_type.clone()))
        .collect::<Vec<_>>();
    let stmt =
        tidb_parser::parse("SELECT * FROM hd USE INDEX (c2c1) WHERE c2 = 1 AND c1 > 1").unwrap();
    let Stmt::Query(query) = &stmt else {
        panic!("not a query")
    };
    let QueryStmt::Select(select) = &**query else {
        panic!("not a select")
    };
    let scope = crate::plan_trace::PlanTrace::single_table_scope("hd", None, columns.clone());
    let Some(crate::driver::access::ChosenPath::Index(_, ranges, _, _)) = choose_index_range_path(
        select,
        &catalog,
        &scope,
        table,
        &columns,
        &crate::index_hints::AvailablePaths::unrestricted(),
        false,
    ) else {
        panic!("expected an index path");
    };
    assert_eq!(
        ranges,
        vec![IndexRange {
            low: vec![Datum::Int(1), Datum::Int(1)],
            high: vec![Datum::Int(1), Datum::MaxValue],
            low_exclusive: true,
            high_exclusive: false,
        }],
        "the handle was appended behind the key part that already holds it"
    );
    let rows = run_select_on(
        "SELECT c1 FROM hd USE INDEX (c2c1) WHERE c2 = 1 AND c1 > 1",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
}

/// A PREFIX key part takes the handle dimension too, and the rows still match
/// a full scan.
///
/// This one was MEASURED against the assumption rather than derived from it:
/// a first reading refused the append behind a prefix, on the theory that the
/// ranger cannot reach the bytes past a cut value. Real TiDB does append, and
/// it is right to: the cut value is a POINT over the key part's own stored
/// bytes, so the handle sits directly behind it in the key.
///
/// ```text
/// create table hp (c1 bigint primary key, s varchar(20), index sp (s(3)));
/// insert into hp values (1,'abcdef'),(2,'abcdef'),(3,'abcdef');
/// explain select * from hp use index(sp) where s = 'abcdef' and c1 > 1;
///   IndexRangeScan_5(Build) | index:sp(s) | range:("abc" 1,"abc" +inf]
/// select c1 from hp use index(sp) where s = 'abcdef' and c1 > 1;  -- 2; 3
/// ```
#[test]
fn a_prefix_key_part_takes_the_handle_dimension_behind_it() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE hp (c1 BIGINT PRIMARY KEY, s VARCHAR(20), INDEX sp (s(3)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO hp VALUES (1, 'abcdef'), (2, 'abcdef'), (3, 'abcdef'), \
         (4, 'abcxyz'), (5, 'zzz')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let ids = |sql: &str| {
        let mut ids: Vec<i64> = run_select_on(sql, &catalog, &crate::StmtContext::for_query())
            .unwrap()
            .into_iter()
            .map(|row| match row[0] {
                Datum::Int(v) => v,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect();
        ids.sort_unstable();
        ids
    };
    // A prefix key part never covers its column, so the residual `s =
    // 'abcdef'` still has to run over the looked-up row -- which is exactly
    // what keeps `abcxyz` (same cut value, different column value) out.
    assert_eq!(
        ids("SELECT c1 FROM hp USE INDEX (sp) WHERE s = 'abcdef' AND c1 > 1"),
        vec![2, 3]
    );
    for predicate in [
        "s = 'abcdef' AND c1 > 1",
        "s = 'abcdef' AND c1 < 3",
        "s = 'abcxyz' AND c1 > 1",
        "s = 'zzz' AND c1 >= 5",
    ] {
        assert_eq!(
            ids(&format!(
                "SELECT c1 FROM hp USE INDEX (sp) WHERE {predicate}"
            )),
            ids(&format!(
                "SELECT c1 FROM hp IGNORE INDEX (sp) WHERE {predicate}"
            )),
            "index and table paths disagree on `{predicate}`"
        );
    }
}

/// A handle that leads its own index is not appended behind it either. This is
/// the `alreadyHandle` case in the other order, and the one where appending
/// twice would build a THREE-datum range over a TWO-datum key and read
/// nothing at all.
///
/// Captured: `index c1c2(c1, c2)` over `c1 BIGINT PRIMARY KEY` ranges
/// `[1 2,1 2]` for `where c1 = 1 and c2 = 2`, and returns the row.
#[test]
fn a_handle_that_leads_its_index_is_not_appended_behind_it() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE hr (c1 BIGINT PRIMARY KEY, c2 INT, INDEX c1c2 (c1, c2))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO hr VALUES (1, 2), (2, 2), (3, 2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let rows = run_select_on(
        "SELECT c1 FROM hr USE INDEX (c1c2) WHERE c1 = 1 AND c2 = 2",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(rows.len(), 1, "the row was lost: {rows:?}");
    assert_eq!(rows[0][0], Datum::Int(1));
}
