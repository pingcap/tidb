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
        match choose_index_range_path(select, &catalog, &scope, table, &columns) {
            Some(crate::driver::access::ChosenPath::Index(id, ranges, _)) => Some((id, ranges)),
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
