//! What a table with a PREFIX index answers, end to end through the driver.
//!
//! # Why the assertions are on VALUES
//!
//! A prefix index entry holds `'abc'` where the row holds `'abcdef'`. Every
//! way of getting this feature wrong returns the same NUMBER of rows and the
//! wrong CONTENT -- an answer counted right and read wrong. So every
//! assertion here compares the returned strings, and the fixture is built so
//! that the prefix and the whole value differ visibly and two rows share a
//! prefix while differing after it. A fixture without that second row cannot
//! tell a correct read from a truncated one.
//!
//! The original prefix fixtures were captured from real TiDB through `gorun`.
//! The partial-order cases also check the pinned master source contract and
//! native lookup counts; they are not fresh master Go execution receipts.
//! Mirrors Go `pkg/tablecodec`'s `TruncateIndexValue`,
//! `pkg/util/ranger`'s `cutPrefixForPoints`, and the covering / ordering /
//! point-get rules in `pkg/planner/core`.

use super::*;

/// `t(a varchar(20), b int, key idx(a(3)))` holding the captured fixture.
///
/// `'abcdef'` and `'abcxyz'` share the indexed prefix `'abc'` and differ
/// after it; `'zzz'` is shorter than nothing else and lands in its own
/// entry.
fn prefix_table() -> Catalog {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE t (a VARCHAR(20), b INT, KEY idx (a(3)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO t VALUES ('abcdef', 1), ('abcxyz', 2), ('zzz', 3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    catalog
}

fn strings(sql: &str, catalog: &Catalog) -> Vec<String> {
    let mut out: Vec<String> = run_select_on(sql, catalog, &crate::StmtContext::for_query())
        .unwrap()
        .iter()
        .map(|row| datum_text_for_test(&row[0]))
        .collect();
    out.sort();
    out
}

/// THE TRUNCATION CASE. Captured from real TiDB:
/// `select a from t where a = 'abcdef'` returns `abcdef`.
///
/// Answering from the index alone would return `'abc'`, and the row count
/// would be identical either way -- which is why this asserts the value.
#[test]
fn selecting_the_indexed_column_returns_the_whole_value_not_the_prefix() {
    let catalog = prefix_table();
    assert_eq!(
        strings("SELECT a FROM t WHERE a = 'abcdef'", &catalog),
        vec!["abcdef".to_owned()]
    );
}

/// THE SHARED-PREFIX CASE. `'abcdef'` and `'abcxyz'` occupy the same index
/// entry, so the scan reaches both and the residual `WHERE` must separate
/// them. Captured: each equality returns exactly its own row.
#[test]
fn two_rows_sharing_a_prefix_are_told_apart_by_the_residual_predicate() {
    let catalog = prefix_table();
    assert_eq!(
        strings("SELECT a FROM t WHERE a = 'abcdef'", &catalog),
        vec!["abcdef".to_owned()]
    );
    assert_eq!(
        strings("SELECT a FROM t WHERE a = 'abcxyz'", &catalog),
        vec!["abcxyz".to_owned()]
    );
}

/// A value that IS the prefix matches no row here: the entries say `'abc'`
/// but the rows say `'abcdef'`/`'abcxyz'`. Captured: `select a from t where
/// a = 'abc'` returns nothing, and `select count(*)` returns 0.
///
/// This is the direction a covering read gets wrong the other way round --
/// it would answer two rows of `'abc'`.
#[test]
fn a_query_for_the_prefix_itself_matches_nothing() {
    let catalog = prefix_table();
    assert!(strings("SELECT a FROM t WHERE a = 'abc'", &catalog).is_empty());
}

/// Inequalities: the cut endpoint loses its exclusiveness, so the scan still
/// reaches the rows behind the prefix and the residual `WHERE` trims them.
/// Captured, in order: three rows, three rows, nothing, and two rows.
#[test]
fn inequalities_over_a_cut_endpoint_still_read_every_qualifying_row() {
    let catalog = prefix_table();
    assert_eq!(
        strings("SELECT a FROM t WHERE a > 'abc'", &catalog),
        vec!["abcdef".to_owned(), "abcxyz".to_owned(), "zzz".to_owned()]
    );
    assert_eq!(
        strings("SELECT a FROM t WHERE a >= 'abcdef'", &catalog),
        vec!["abcdef".to_owned(), "abcxyz".to_owned(), "zzz".to_owned()]
    );
    assert!(strings("SELECT a FROM t WHERE a < 'abcdef'", &catalog).is_empty());
    assert_eq!(
        strings("SELECT a FROM t WHERE a IN ('abcdef', 'zzz')", &catalog),
        vec!["abcdef".to_owned(), "zzz".to_owned()]
    );
}

/// A UNIQUE prefix index enforces uniqueness ON THE PREFIX. Captured:
/// `insert into u values ('abcxyz')` after `('abcdef')` is rejected, and the
/// table keeps only the first row.
///
/// The reported value is the CUT one, which is how Go builds the message
/// (`TruncateIndexValues` then `genIndexKeyStrs` in `pkg/table/tables`).
#[test]
fn a_unique_prefix_index_rejects_a_row_that_only_shares_the_prefix() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE u (a VARCHAR(20), UNIQUE KEY uidx (a(3)))",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_query();
    run_insert_on("INSERT INTO u VALUES ('abcdef')", &mut catalog, &ctx).unwrap();
    let error = run_insert_on("INSERT INTO u VALUES ('abcxyz')", &mut catalog, &ctx)
        .expect_err("a row sharing the indexed prefix must be rejected");
    assert!(
        matches!(&error, DriverError::DuplicateEntry { value, .. } if value == "abc"),
        "{error:?}"
    );
    // A value that differs inside the prefix is accepted.
    run_insert_on("INSERT INTO u VALUES ('abdefg')", &mut catalog, &ctx).unwrap();
    assert_eq!(
        strings("SELECT a FROM u", &catalog),
        vec!["abcdef".to_owned(), "abdefg".to_owned()]
    );
}

/// A unique prefix index must not become a POINT GET: the entry found by
/// `'abcxyz'`'s prefix belongs to `'abcdef'`, and a point get has no residual
/// predicate to notice. Captured: real TiDB plans an `IndexLookUp` with a
/// `Selection`, not a `Point_Get`.
///
/// Asserted through the ROWS, because that is where the bug would show: a
/// point get would answer `'abcdef'` for a query about `'abcxyz'`.
#[test]
fn a_unique_prefix_index_does_not_answer_a_point_get() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE u (a VARCHAR(20), b INT, UNIQUE KEY uidx (a(3)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO u VALUES ('abcdef', 1), ('zzz', 2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert!(strings("SELECT a FROM u WHERE a = 'abcxyz'", &catalog).is_empty());
    assert_eq!(
        strings("SELECT a FROM u WHERE a = 'abcdef'", &catalog),
        vec!["abcdef".to_owned()]
    );
    // The `IN` form reaches the batch point get instead, and must decline it
    // for the same reason.
    assert_eq!(
        strings("SELECT a FROM u WHERE a IN ('abcxyz', 'zzz')", &catalog),
        vec!["zzz".to_owned()]
    );
}

/// An index scan over a prefix key part walks PREFIX order, which is not the
/// column's order, so it cannot discharge an `ORDER BY`. Captured: real TiDB
/// plans `Sort` over `TableFullScan` for `select a from t order by a`.
///
/// Asserted through the rows a `LIMIT` returns, which is the shape the bug
/// takes here: a scan wrongly believed ordered stops after `n` entries and
/// answers whichever rows those were.
#[test]
fn an_order_by_is_not_satisfied_by_a_prefix_key_part() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE o (a VARCHAR(20), KEY idx (a(3)))",
        &mut catalog,
    )
    .unwrap();
    // The rows arrive in an order that disagrees with both the prefix order
    // and the whole-value order, so a scan that trusted the index would
    // answer the wrong first row.
    run_insert_on(
        "INSERT INTO o VALUES ('abczz'), ('abcaa'), ('aaa')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    // The `WHERE` is what makes the index path a candidate at all -- without
    // it the covering test alone already drops the path, and this would be
    // measuring that rule rather than the ORDER BY one. `'abczz'` and
    // `'abcaa'` share one entry, so index order puts whichever was written
    // first ahead of the other and a pushed `LIMIT 2` would answer
    // `aaa, abczz`.
    let rows = run_select_on(
        "SELECT a FROM o WHERE a > 'a' ORDER BY a LIMIT 2",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        rows.iter()
            .map(|row| datum_text_for_test(&row[0]))
            .collect::<Vec<_>>(),
        vec!["aaa".to_owned(), "abcaa".to_owned()]
    );
}

/// A composite index whose FIRST part is whole still orders by that part, so
/// only the tail is lost. Go's `matchIndicesProp` reaches the same answer by
/// rejecting the property at the first key part with a length.
#[test]
fn a_leading_whole_key_part_still_orders_by_itself() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE c (a INT, b VARCHAR(20), KEY idx (a, b(3)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO c VALUES (3, 'zzz'), (1, 'abczz'), (2, 'abcaa')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let first = run_select_on(
        "SELECT a FROM c WHERE a > 0 ORDER BY a LIMIT 2",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|row| match row[0] {
                Datum::Int(value) => value,
                ref other => panic!("expected an int, got {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![1, 2]
    );
    // The prefixed second part orders nothing, so the whole ORDER BY sorts.
    let both = run_select_on(
        "SELECT b FROM c WHERE a > 0 ORDER BY a, b LIMIT 2",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        both.iter()
            .map(|row| datum_text_for_test(&row[0]))
            .collect::<Vec<_>>(),
        vec!["abczz".to_owned(), "abcaa".to_owned()]
    );
}

/// DELETE and UPDATE maintain the cut entries, so the index stays consistent
/// with the rows: `ADMIN CHECK TABLE` is the oracle, and it compares each
/// stored entry against one re-encoded from the row it names. Captured: all
/// three `admin check table` runs pass on real TiDB.
#[test]
fn writes_through_a_prefix_index_stay_admin_check_clean() {
    let mut catalog = prefix_table();
    let ctx = crate::StmtContext::for_query();
    // `check_table` returns the number of INDEXES it checked; the row-level
    // agreement is what it errors on.
    let check = |catalog: &mut Catalog| {
        let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t") else {
            panic!("the table is not storage-backed");
        };
        assert_eq!(
            crate::admin_check::check_table(
                std::sync::Arc::make_mut(table),
                None,
                &crate::RowDecodeContext::for_test_query_utc(),
            )
            .expect("every stored entry re-encodes from the row it names"),
            1
        );
    };
    check(&mut catalog);

    run_delete_on("DELETE FROM t WHERE a = 'abcdef'", &mut catalog, &ctx).unwrap();
    check(&mut catalog);
    assert_eq!(
        strings("SELECT a FROM t", &catalog),
        vec!["abcxyz".to_owned(), "zzz".to_owned()]
    );

    run_update_on("UPDATE t SET a = 'abcqqq' WHERE b = 2", &mut catalog, &ctx).unwrap();
    check(&mut catalog);
    assert_eq!(
        strings("SELECT a FROM t", &catalog),
        vec!["abcqqq".to_owned(), "zzz".to_owned()]
    );
}

/// A multi-byte charset counts CHARACTERS: `(3)` over `utf8mb4` keeps three
/// code points. Captured: `select a from c where a = '世界你好啊'` returns
/// the whole value and `admin check table c` passes.
#[test]
fn a_multi_byte_column_is_cut_by_characters() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE c (a VARCHAR(20), KEY idx (a(3)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO c VALUES ('世界你好啊'), ('世界你不好')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        strings("SELECT a FROM c WHERE a = '世界你好啊'", &catalog),
        vec!["世界你好啊".to_owned()]
    );
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in("test", "c") else {
        panic!("the table is not storage-backed");
    };
    assert_eq!(
        crate::admin_check::check_table(
            std::sync::Arc::make_mut(table),
            None,
            &crate::RowDecodeContext::for_test_query_utc(),
        )
        .unwrap(),
        1
    );
}

/// `CREATE INDEX` backfills the cut entries from the rows that already exist,
/// and the reads that follow are the same reads a table created with the
/// index would give.
#[test]
fn create_index_backfills_cut_entries() {
    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query();
    crate::run_create_table_on("CREATE TABLE b (a VARCHAR(20), c INT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO b VALUES ('abcdef', 1), ('abcxyz', 2)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    crate::ddl::run_create_index_in("CREATE INDEX idx ON b (a(3))", &mut catalog, "test", &ctx)
        .unwrap();
    assert_eq!(
        strings("SELECT a FROM b WHERE a = 'abcxyz'", &catalog),
        vec!["abcxyz".to_owned()]
    );
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in("test", "b") else {
        panic!("the table is not storage-backed");
    };
    assert_eq!(
        crate::admin_check::check_table(
            std::sync::Arc::make_mut(table),
            None,
            &crate::RowDecodeContext::for_test_query_utc(),
        )
        .unwrap(),
        1
    );
}

/// `MODIFY COLUMN` off a prefixable type -- or onto one no wider than the
/// prefix -- clears the key part's declared length, and the entries are
/// rewritten to match. Go `ddl.UpdateIndexCol` (`pkg/ddl/column.go`).
///
/// Captured from real TiDB's `SHOW CREATE TABLE` after each of these:
/// `UNIQUE KEY idx (a)` in all three, with no `(n)`.
#[test]
fn modify_column_clears_a_prefix_the_new_type_cannot_carry() {
    for (create, modify) in [
        ("CREATE TABLE t (a TEXT, UNIQUE INDEX idx (a(2)))", "a INT"),
        (
            "CREATE TABLE t (a CHAR(255), UNIQUE INDEX idx (a(2)))",
            "a FLOAT",
        ),
        // The new width is no wider than the prefix, so the key part covers
        // the whole column even though the type is still prefixable.
        (
            "CREATE TABLE t (a CHAR(250), UNIQUE KEY idx (a(10)))",
            "a CHAR(9)",
        ),
    ] {
        let mut catalog = Catalog::default();
        crate::run_create_table_on(create, &mut catalog).expect(create);
        crate::ddl::run_alter_table_in(
            &format!("ALTER TABLE t MODIFY COLUMN {modify}"),
            &mut catalog,
            "test",
            &crate::StmtContext::for_query(),
        )
        .expect(modify);
        let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t") else {
            panic!("{create}: the table is not storage-backed");
        };
        assert!(
            !table.indexes()[0].has_prefix(),
            "{create} -> {modify}: the key part still declares a prefix"
        );
        // The entries were rebuilt under the new (absent) length, so the
        // index still agrees with the rows.
        crate::admin_check::check_table(
            std::sync::Arc::make_mut(table),
            None,
            &crate::RowDecodeContext::for_test_query_utc(),
        )
        .expect("the rebuilt entries match the rows");
    }
}

/// A prefix the new type CAN still carry survives, and the entries stay cut.
/// Go keeps `ic.Length` when the type is prefixable and wider than it, which
/// is why the refusal below is about the LENGTH and not about the type:
/// `TEXT` with a `(2)` key part is legal, and `TEXT` with none is 1170.
#[test]
fn modify_column_keeps_a_prefix_the_new_type_can_carry() {
    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query();
    crate::run_create_table_on(
        "CREATE TABLE t (a VARCHAR(255), KEY idx (a(2)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on("INSERT INTO t VALUES ('abcdef')", &mut catalog, &ctx).unwrap();
    crate::ddl::run_alter_table_in(
        "ALTER TABLE t MODIFY COLUMN a TEXT",
        &mut catalog,
        "test",
        &ctx,
    )
    .unwrap();
    assert_eq!(
        strings("SELECT a FROM t WHERE a = 'abcdef'", &catalog),
        vec!["abcdef".to_owned()]
    );
    let Some(crate::TableEntry::Kv(table)) = catalog.table_mut_in("test", "t") else {
        panic!("the table is not storage-backed");
    };
    assert_eq!(table.indexes()[0].prefix_lengths, vec![2]);
    crate::admin_check::check_table(
        std::sync::Arc::make_mut(table),
        None,
        &crate::RowDecodeContext::for_test_query_utc(),
    )
    .expect("the cut entries match the rows");

    // The same column with NO surviving prefix is Go's 1170.
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE u (a VARCHAR(255), KEY idx (a))", &mut catalog)
        .unwrap();
    assert!(matches!(
        crate::ddl::run_alter_table_in(
            "ALTER TABLE u MODIFY COLUMN a TEXT",
            &mut catalog,
            "test",
            &ctx
        ),
        Err(DriverError::BlobKeyWithoutLength(ref column)) if column == "a"
    ));
}

#[test]
fn partial_order_topn_keeps_competing_rows_with_the_same_prefix() {
    use crate::explain::{explain_select_stmt, ExplainFormat};

    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query().with_partial_ordered_index_for_topn(true);
    crate::run_create_table_on(
        "CREATE TABLE p (a VARCHAR(20), KEY idx(a(3)))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO p VALUES ('abczz'), ('abcaa'), ('abcmm'), ('aaa'), ('zzz')",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    for (order, expected) in [
        ("ASC", vec!["abcaa", "abcmm"]),
        ("DESC", vec!["abczz", "abcmm"]),
    ] {
        for (hint, clause, enabled, prefix) in [
            ("/*+ ORDER_INDEX(p, idx) */", "", true, true),
            ("", "USE INDEX(idx)", true, true),
            ("", "FORCE INDEX(idx)", true, true),
            ("/*+ USE_INDEX(p, idx) */", "", true, true),
            (
                "/*+ NO_ORDER_INDEX(p, idx) */",
                "USE INDEX(idx)",
                true,
                false,
            ),
            ("", "USE INDEX(idx)", false, false),
        ] {
            let ctx = crate::StmtContext::for_query().with_partial_ordered_index_for_topn(enabled);
            let sql = format!("SELECT {hint} a FROM p {clause} ORDER BY a {order} LIMIT 1, 2");
            let (rows, ops) =
                crate::storage::capture_storage_ops(|| run_select_on(&sql, &catalog, &ctx));
            let rows = rows.unwrap();
            assert_eq!(
                ops.gets,
                if prefix { 4 } else { 5 },
                "{sql}: prefix optimization={prefix}: {ops:?}"
            );
            assert_eq!(
                rows.iter()
                    .map(|row| datum_text_for_test(&row[0]))
                    .collect::<Vec<_>>(),
                expected
            );
            let stmt = tidb_parser::parse(&sql).unwrap();
            let Stmt::Query(query) = &stmt else {
                panic!("query");
            };
            let QueryStmt::Select(select) = &**query else {
                panic!("select");
            };
            let (_, plan) =
                explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
            let text = plan
                .iter()
                .flatten()
                .map(|datum| match datum {
                    Datum::Bytes(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                    other => format!("{other:?}"),
                })
                .collect::<Vec<_>>()
                .join(" ");
            assert_eq!(text.contains("prefix_col:"), prefix, "{sql}: {text}");
        }
    }
}

#[test]
fn partial_order_prefix_limit_handles_null_collation_composite_keys_and_filters() {
    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query().with_partial_ordered_index_for_topn(true);
    for (table, ddl, insert, sql, expected, gets) in [
        (
            "pn",
            "a VARCHAR(20), KEY idx(a(2))",
            "(NULL), (NULL), ('zz')",
            "SELECT /*+ ORDER_INDEX(pn, idx) */ a FROM pn ORDER BY a LIMIT 1, 1",
            None,
            2,
        ),
        (
            "pc",
            "a VARCHAR(20) COLLATE utf8mb4_general_ci, KEY idx(a(2))",
            "('abz'), ('ABa'), ('zz')",
            "SELECT /*+ ORDER_INDEX(pc, idx) */ a FROM pc ORDER BY a LIMIT 1",
            Some("ABa"),
            2,
        ),
        (
            "pu",
            "a VARCHAR(20) COLLATE utf8mb4_bin, KEY idx(a(2))",
            "('猫咪z'), ('猫咪a'), ('狗狗b')",
            "SELECT /*+ ORDER_INDEX(pu, idx) */ a FROM pu ORDER BY a LIMIT 1, 1",
            Some("猫咪a"),
            3,
        ),
        (
            "pm",
            "id INT, a VARCHAR(20), KEY idx(id, a(2))",
            "(1, 'abz'), (1, 'aba'), (2, 'zz')",
            "SELECT /*+ ORDER_INDEX(pm, idx) */ a FROM pm ORDER BY id, a LIMIT 1",
            Some("aba"),
            2,
        ),
        (
            "pi",
            "id INT PRIMARY KEY, a VARCHAR(20), KEY idx(a(2))",
            "(1, 'abz'), (2, 'abb'), (3, 'aba'), (4, 'zz')",
            "SELECT /*+ ORDER_INDEX(pi, idx) */ a FROM pi WHERE id > 1 ORDER BY a LIMIT 1",
            Some("aba"),
            2,
        ),
        (
            "pg",
            "k VARCHAR(20) COLLATE utf8mb4_general_ci PRIMARY KEY CLUSTERED, a VARCHAR(20), KEY idx(a(2))",
            "('KeyA', 'abz'), ('KeyB', 'abb'), ('KeyC', 'aba'), ('KeyD', 'zz')",
            "SELECT /*+ ORDER_INDEX(pg, idx) */ a FROM pg WHERE k > 'KeyA' ORDER BY a LIMIT 1",
            Some("aba"),
            2,
        ),
        (
            "pd",
            "a VARCHAR(20) PRIMARY KEY CLUSTERED, b VARCHAR(20), KEY idx(a(2))",
            "('abz', '1'), ('abb', '2'), ('aba', '3'), ('zz', '4')",
            "SELECT b FROM pd USE INDEX(idx) WHERE a = 'aba' LIMIT 1",
            Some("3"),
            1,
        ),
        (
            "pt",
            "a VARCHAR(20), b INT, KEY idx(a(2))",
            "('abz', 0), ('abb', 1), ('aba', 1), ('zz', 1)",
            "SELECT /*+ ORDER_INDEX(pt, idx) */ a FROM pt WHERE b > 0 ORDER BY a LIMIT 1",
            Some("aba"),
            3,
        ),
    ] {
        crate::run_create_table_on(&format!("CREATE TABLE {table} ({ddl})"), &mut catalog).unwrap();
        run_insert_on(
            &format!("INSERT INTO {table} VALUES {insert}"),
            &mut catalog,
            &ctx,
        )
        .unwrap();
        let (rows, ops) =
            crate::storage::capture_storage_ops(|| run_select_on(sql, &catalog, &ctx));
        let rows = rows.unwrap_or_else(|error| panic!("{sql}: {error:?}"));
        assert_eq!(rows.len(), 1, "{sql}");
        match expected {
            Some(expected) => assert_eq!(datum_text_for_test(&rows[0][0]), expected, "{sql}"),
            None => assert!(matches!(rows[0][0], Datum::Null), "{sql}"),
        }
        assert_eq!(ops.gets, gets, "{sql}: {ops:?}");
        if table == "pi" {
            let rows = run_select_on(
                "SELECT a FROM pi USE INDEX(idx) WHERE id > 1 LIMIT 1",
                &catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();
            assert_eq!(
                rows.len(), 1,
                "an ordinary lookup Limit also counts after the index predicate"
            );
            assert_eq!(datum_text_for_test(&rows[0][0]), "abb");
            let rows = run_select_on(
                "SELECT a FROM pi USE INDEX(idx) WHERE id > 1 LIMIT 1, 1",
                &catalog,
                &crate::StmtContext::for_query(),
            )
            .unwrap();
            assert_eq!(rows.len(), 1);
            assert_eq!(datum_text_for_test(&rows[0][0]), "aba");
        }
    }
}

#[test]
fn prefix_null_conditions_use_a_covering_reader_without_exposing_prefix_values() {
    use crate::explain::{explain_select_stmt, ExplainFormat};
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE pn (a VARCHAR(20), b INT, KEY idx(a(2), b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO pn VALUES (NULL, 1), ('abz', 2), ('aba', 3), ('zz', 4)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    for enabled in [true, false] {
        let ctx = crate::StmtContext::for_query().with_opt_prefix_index_single_scan(enabled);
        for (projection, condition, expected, null_only) in [
            ("b", "a IS NULL", vec!["1"], true),
            ("b", "a IS NOT NULL", vec!["2", "3", "4"], true),
            ("b", "(a IS NULL) = 0", vec!["2", "3", "4"], true),
            ("b", "a IS NULL OR b = 3", vec!["1", "3"], true),
            ("b", "a IS NULL AND b > 0", vec!["1"], true),
            ("b", "CHAR_LENGTH(a) IS NULL", vec!["1"], false),
            ("b", "a IS NULL OR a = 'aba'", vec!["1", "3"], false),
            ("a", "a IS NOT NULL", vec!["aba", "abz", "zz"], false),
        ] {
            let sql = format!("SELECT {projection} FROM pn USE INDEX(idx) WHERE {condition}");
            let (rows, ops) =
                crate::storage::capture_storage_ops(|| run_select_on(&sql, &catalog, &ctx));
            let mut rows: Vec<_> = rows
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
                .iter()
                .map(|row| match &row[0] {
                    Datum::Int(value) => value.to_string(),
                    value => datum_text_for_test(value),
                })
                .collect();
            rows.sort();
            assert_eq!(rows, expected, "{sql}, enabled={enabled}");
            let Stmt::Query(query) = tidb_parser::parse(&sql).unwrap() else {
                panic!("query")
            };
            let QueryStmt::Select(select) = &*query else {
                panic!("select")
            };
            let (_, plan) =
                explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
            let text = plan
                .iter()
                .map(|row| datum_text_for_test(&row[0]))
                .collect::<Vec<_>>()
                .join(" ");
            let covering = enabled && null_only;
            assert_eq!(
                text.contains("IndexReader"),
                covering,
                "{sql}, enabled={enabled}: {text}"
            );
            if covering {
                assert_eq!(
                    ops.gets, 0,
                    "a covering null predicate must not fetch table rows: {sql}, {ops:?}"
                );
            }
        }
    }
}

#[test]
fn index_join_covering_rows_match_master_with_prefix_filters_and_outer_nulls() {
    use crate::explain::{explain_select_stmt, ExplainFormat};
    let mut catalog = Catalog::default();
    let ctx = crate::StmtContext::for_query();
    for ddl in [
        "CREATE TABLE o (k INT)",
        "CREATE TABLE i (k INT, a VARCHAR(20), b INT, KEY idx(k, a(2), b))",
    ] {
        crate::run_create_table_on(ddl, &mut catalog).unwrap();
    }
    for insert in [
        "INSERT INTO o VALUES(1),(3),(NULL),(3),(9)",
        "INSERT INTO i VALUES(1,NULL,10),(1,'abz',11),(3,NULL,30),(3,'aba',31),(3,'abb',32)",
    ] {
        run_insert_on(insert, &mut catalog, &ctx).unwrap();
    }
    // Captured from TiDB master 64e8c4c05e. Join reorder derives path coverage
    // before final pruning removes the null predicate's full-length column.
    let cases = [
        (
            "SELECT /*+ INL_JOIN(i) */ o.k,i.b FROM o LEFT JOIN i USE INDEX(idx) ON o.k=i.k WHERE i.a IS NULL",
            vec!["1 10", "3 30", "3 30", "9 <nil>", "<nil> <nil>"],
            false,
        ),
        (
            "SELECT /*+ INL_JOIN(i) */ o.k,i.b FROM o JOIN i USE INDEX(idx) ON o.k=i.k WHERE i.a IS NOT NULL",
            vec!["1 11", "3 31", "3 31", "3 32", "3 32"],
            false,
        ),
        (
            "SELECT /*+ INL_JOIN(i) */ o.k,i.a FROM o JOIN i USE INDEX(idx) ON o.k=i.k WHERE i.a IS NOT NULL",
            vec!["1 abz", "3 aba", "3 aba", "3 abb", "3 abb"],
            false,
        ),
        (
            "SELECT /*+ INL_JOIN(i) */ o.k,i.b FROM o JOIN i USE INDEX(idx) ON o.k=i.k WHERE i.b>10",
            vec!["1 11", "3 30", "3 30", "3 31", "3 31", "3 32", "3 32"],
            true,
        ),
    ];
    for (dirty, ctx) in [(false, crate::StmtContext::for_query()), (true, ctx)] {
        for (sql, expected, covering) in cases.clone() {
            let (rows, ops) =
                crate::storage::capture_storage_ops(|| run_select_on(sql, &catalog, &ctx));
            let mut rows: Vec<_> = rows
                .unwrap_or_else(|error| panic!("{sql}: {error:?}"))
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|value| match value {
                            Datum::Null => "<nil>".to_owned(),
                            Datum::Int(value) => value.to_string(),
                            value => datum_text_for_test(value),
                        })
                        .collect::<Vec<_>>()
                        .join(" ")
                })
                .collect();
            rows.sort();
            assert_eq!(rows, expected, "{sql}");
            let Stmt::Query(query) = tidb_parser::parse(sql).unwrap() else {
                panic!("query")
            };
            let QueryStmt::Select(select) = &*query else {
                panic!("select")
            };
            let (_, plan) =
                explain_select_stmt(select, &catalog, "test", &ctx, ExplainFormat::Brief).unwrap();
            let plan = plan
                .iter()
                .map(|row| datum_text_for_test(&row[0]))
                .collect::<Vec<_>>()
                .join(" ");
            assert_eq!(plan.contains("IndexReader"), covering, "{sql}: {plan}");
            if covering && !dirty {
                assert_eq!(
                    ops.gets, 0,
                    "covering inner reader must avoid table gets: {ops:?}"
                );
            }
        }
    }
}
