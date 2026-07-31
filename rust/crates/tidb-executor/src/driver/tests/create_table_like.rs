//! `CREATE TABLE ... LIKE`: the structure a new table inherits from another,
//! and -- the half where the bugs live -- what it deliberately does NOT
//! inherit.
//!
//! Mirrors Go `pkg/ddl/create_table.go`'s `BuildTableInfoWithLike`. That
//! function shallow-copies the whole `TableInfo` and then resets four things:
//! the name, `AutoIncID`, `ForeignKeys`, and the cache/replica status. The
//! rows are never in `TableInfo` at all, which is why the copy starts empty.
//!
//! Every assertion here was captured from real TiDB through `gorun` before it
//! was written, including the two that pin what is absent: the copy's
//! auto-increment counter restarts at 1 even when the source's has run to
//! 30100, and the copy declares none of the source's foreign keys.

use super::*;

/// The `KvTable` a test asserts structure against.
fn kv<'a>(catalog: &'a Catalog, name: &str) -> &'a crate::KvTable {
    match catalog.get_table_for_test(name) {
        Some(crate::driver::catalog::TableEntry::Kv(kv)) => kv,
        _ => panic!("{name} is not a KV table"),
    }
}

/// The columns and indexes are inherited, and the copy starts EMPTY. Go
/// copies `TableInfo`, which never held a row.
#[test]
fn like_copies_the_structure_and_none_of_the_rows() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE src (a BIGINT, b VARCHAR(10), KEY k (a), UNIQUE KEY u (b))",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO src VALUES (1, 'x'), (2, 'y')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    crate::run_create_table_on("CREATE TABLE dst LIKE src", &mut catalog).unwrap();

    // Structure inherited: both indexes, by name and uniqueness.
    let dst = kv(&catalog, "dst");
    assert_eq!(dst.columns.len(), 2);
    let indexes: Vec<(&str, bool)> = dst
        .indexes()
        .iter()
        .map(|index| (index.name.as_str(), index.unique))
        .collect();
    assert_eq!(indexes, vec![("k", false), ("u", true)]);

    // Rows NOT inherited.
    assert_eq!(
        run_select_on(
            "SELECT a FROM dst",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        0
    );

    // The copy is a separate table: writing it leaves the source alone.
    run_insert_on(
        "INSERT INTO dst VALUES (9, 'z')",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a FROM src",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        2
    );
}

/// Go `tblInfo.AutoIncID = 0`. Captured: `src` reaches `AUTO_INCREMENT=30100`
/// and `SHOW CREATE TABLE dst` prints no `AUTO_INCREMENT` clause at all, so
/// the copy's first insert is id 1 -- NOT a continuation of the source's
/// counter. This is the reset that a shallow struct copy would silently lose.
#[test]
fn like_does_not_copy_the_auto_increment_value() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE src (id BIGINT PRIMARY KEY AUTO_INCREMENT, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    for _ in 0..3 {
        run_insert_on(
            "INSERT INTO src (v) VALUES (1)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap();
    }

    crate::run_create_table_on("CREATE TABLE dst LIKE src", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO dst (v) VALUES (7)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    assert_eq!(
        run_select_on(
            "SELECT id FROM dst",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
}

/// Go `tblInfo.ForeignKeys = nil`. The copy declares no constraint, so a row
/// the source would reject for having no parent is accepted here.
#[test]
fn like_does_not_copy_foreign_keys() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE parent (id BIGINT PRIMARY KEY)", &mut catalog)
        .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE child (id BIGINT PRIMARY KEY, pid BIGINT, \
         FOREIGN KEY (pid) REFERENCES parent (id))",
        &mut catalog,
    )
    .unwrap();

    crate::run_create_table_on("CREATE TABLE copy_of_child LIKE child", &mut catalog).unwrap();

    assert!(kv(&catalog, "copy_of_child").foreign_keys().is_empty());
    // The source still enforces its own.
    assert!(!kv(&catalog, "child").foreign_keys().is_empty());
}

/// Go `BuildTableInfoWithLike`: `referTblInfo.IsSequence() || IsView()` is
/// `ErrWrongObject` (1347), whose third argument here is `BASE TABLE`.
#[test]
fn like_a_view_is_wrong_object() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE t (a BIGINT)", &mut catalog).unwrap();
    catalog.register_view_in(
        "test",
        "v",
        crate::driver::catalog::ViewDef {
            name: "v".to_owned(),
            columns: vec![("a".to_owned(), FieldType::new(FieldTypeCode::LongLong))],
            select_sql: "SELECT `a` AS `a` FROM `test`.`t`".to_owned(),
            definer_user: String::new(),
            definer_host: String::new(),
            algorithm: "UNDEFINED".to_owned(),
            security: "DEFINER".to_owned(),
            check_option: "CASCADED".to_owned(),
        },
    );

    assert!(matches!(
        crate::run_create_table_on("CREATE TABLE dst LIKE v", &mut catalog),
        Err(DriverError::Schema(crate::SchemaErrorKind::WrongObject {
            ref name,
            expected: "BASE TABLE",
        })) if name == "test.v"
    ));
}

/// A source that does not exist is Go's `ErrTableNotExists` (1146), and an
/// existing TARGET is still 1050 -- `IF NOT EXISTS` suppresses it, as it does
/// for an ordinary CREATE.
#[test]
fn like_reports_missing_source_and_existing_target() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on("CREATE TABLE t (a BIGINT)", &mut catalog).unwrap();

    assert!(matches!(
        crate::run_create_table_on("CREATE TABLE dst LIKE nosuch", &mut catalog),
        Err(DriverError::Schema(crate::SchemaErrorKind::UnknownTable(ref name)))
            if name == "test.nosuch"
    ));

    crate::run_create_table_on("CREATE TABLE dst LIKE t", &mut catalog).unwrap();
    assert!(matches!(
        crate::run_create_table_on("CREATE TABLE dst LIKE t", &mut catalog),
        Err(DriverError::Schema(crate::SchemaErrorKind::TableExists(ref name)))
            if name == "test.dst"
    ));
    // `IF NOT EXISTS` reports no error and creates nothing.
    assert!(
        !crate::run_create_table_on("CREATE TABLE IF NOT EXISTS dst LIKE t", &mut catalog).unwrap()
    );
}

/// Go deep-copies `PartitionInfo.Definitions` for the copy. The partitioning
/// is inherited, but each partition is a distinct PHYSICAL table, so the
/// copy's definitions must not reuse the source's ids -- two tables writing
/// records under one physical id would interleave their rows.
#[test]
fn like_copies_partitioning_with_fresh_physical_ids() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE src (a BIGINT) PARTITION BY HASH (a) PARTITIONS 4",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on("CREATE TABLE dst LIKE src", &mut catalog).unwrap();

    let source_ids: Vec<i64> = kv(&catalog, "src")
        .partition()
        .unwrap()
        .definitions
        .iter()
        .map(|def| def.id)
        .collect();
    let copy = kv(&catalog, "dst");
    let copy_partition = copy.partition().expect("the partitioning is inherited");
    let copy_ids: Vec<i64> = copy_partition
        .definitions
        .iter()
        .map(|def| def.id)
        .collect();

    assert_eq!(copy_ids.len(), 4);
    assert_eq!(
        copy_partition
            .definitions
            .iter()
            .map(|def| def.name.as_str())
            .collect::<Vec<_>>(),
        vec!["p0", "p1", "p2", "p3"]
    );
    for id in &copy_ids {
        assert!(
            !source_ids.contains(id),
            "a copied partition reused the source's physical id {id}"
        );
        assert_ne!(*id, copy.table_id);
    }

    // The rows land in the copy's own partitions, not the source's.
    run_insert_on(
        "INSERT INTO dst VALUES (1), (2), (3)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a FROM src",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap()
        .len(),
        0
    );
}
