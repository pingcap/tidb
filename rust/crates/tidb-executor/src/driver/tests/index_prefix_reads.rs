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
//! Every expectation was captured from real TiDB through `gorun` before it
//! was written down. Mirrors Go `pkg/tablecodec`'s `TruncateIndexValue`,
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
            crate::admin_check::check_table(table, None)
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
    assert_eq!(crate::admin_check::check_table(table, None).unwrap(), 1);
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
    assert_eq!(crate::admin_check::check_table(table, None).unwrap(), 1);
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
        crate::admin_check::check_table(table, None).expect("the rebuilt entries match the rows");
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
    crate::admin_check::check_table(table, None).expect("the cut entries match the rows");

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
