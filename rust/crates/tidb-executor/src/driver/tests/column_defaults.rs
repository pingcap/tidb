//! The values a column supplies when a statement does not: `DEFAULT`, `NOT
//! NULL` rejection, and `AUTO_INCREMENT`.
//!
//! Mirrors Go `pkg/table`'s `GetColDefaultValue`/`GetColOriginDefaultValue`
//! and `pkg/meta/autoid`'s allocation, including the ids a rollback does NOT
//! hand back.

use super::*;

/// Column defaults and the NOT NULL rules, following Go's fillColValue
/// and CheckNotNull: an omitted column takes its DEFAULT, an omitted NOT
/// NULL column with no DEFAULT is ErrNoDefaultForField, and an explicit
/// NULL into a NOT NULL column is the different ErrColumnCantNull.
#[test]
fn column_defaults_and_not_null() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE d (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, \
         w BIGINT DEFAULT 7, s VARCHAR(4) DEFAULT 'zz', plain BIGINT)",
        &mut catalog,
    )
    .unwrap();

    // Omitted columns take their defaults; a nullable one with no DEFAULT
    // is NULL.
    run_insert_on(
        "INSERT INTO d (id, n) VALUES (1, 5)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    let row = &run_select_on(
        "SELECT w, s, plain FROM d WHERE id = 1",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap()[0];
    assert_eq!(row[0], Datum::Int(7));
    assert_eq!(datum_text_for_test(&row[1]), "zz");
    assert_eq!(row[2], Datum::Null);

    // An explicit value overrides the default.
    run_insert_on(
        "INSERT INTO d (id, n, w) VALUES (2, 5, 100)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT w FROM d WHERE id = 2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(100)]]
    );

    // An omitted NOT NULL column with no default is 1364.
    assert!(matches!(
        run_insert_on("INSERT INTO d (id) VALUES (3)", &mut catalog, &crate::StmtContext::for_query()),
        Err(DriverError::NoDefaultForField(name)) if name == "n"
    ));
    // An explicit NULL into that column is the other error, 1048.
    assert!(matches!(
        run_insert_on("INSERT INTO d (id, n) VALUES (3, NULL)", &mut catalog, &crate::StmtContext::for_query()),
        Err(DriverError::ColumnCannotBeNull(name)) if name == "n"
    ));
    // A NULL into a nullable column is fine.
    run_insert_on(
        "INSERT INTO d (id, n, plain) VALUES (3, 5, NULL)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // A DEFAULT NULL column is not the same as no DEFAULT: it is
    // omittable even when the column is otherwise unconstrained.
    crate::run_create_table_on(
        "CREATE TABLE e (id BIGINT PRIMARY KEY, v BIGINT DEFAULT NULL)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO e (id) VALUES (1)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT v FROM e",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Null]]
    );

    // A primary key is NOT NULL, so omitting it is 1364 as well.
    assert!(matches!(
        run_insert_on(
            "INSERT INTO e (v) VALUES (1)",
            &mut catalog,
            &crate::StmtContext::for_query()
        ),
        Err(DriverError::NoDefaultForField(_))
    ));

    // An AUTO_INCREMENT column supplies its own value, so omitting it is
    // never the missing-default case (see the auto_increment test).
    crate::run_create_table_on("CREATE TABLE f (a BIGINT AUTO_INCREMENT)", &mut catalog).unwrap();
    run_insert_on(
        "INSERT INTO f () VALUES ()",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .or_else(|_| {
        run_insert_on(
            "INSERT INTO f VALUES (NULL)",
            &mut catalog,
            &crate::StmtContext::for_query(),
        )
    })
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a FROM f",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)]]
    );
    // A generated column is still rejected rather than ignored.
    assert!(crate::run_create_table_on(
        "CREATE TABLE g2 (a BIGINT, b BIGINT GENERATED ALWAYS AS (a+1) VIRTUAL)",
        &mut catalog
    )
    .is_err());
}

/// AUTO_INCREMENT, checked against behavior captured from real TiDB:
/// inserting 1,2 then an explicit 100 rebases the allocator, so the next
/// rows are 101, 102, 103 -- NULL and 0 both allocate.
#[test]
fn auto_increment() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE a1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 (v) VALUES (10), (20)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 VALUES (100, 30)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 (v) VALUES (40)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 VALUES (NULL, 50), (0, 60)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();

    // Captured from TiDB: [[1 10] [2 20] [100 30] [101 40] [102 50] [103 60]]
    assert_eq!(
        run_select_on(
            "SELECT id, v FROM a1",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(10)],
            vec![Datum::Int(2), Datum::Int(20)],
            vec![Datum::Int(100), Datum::Int(30)],
            vec![Datum::Int(101), Datum::Int(40)],
            vec![Datum::Int(102), Datum::Int(50)],
            vec![Datum::Int(103), Datum::Int(60)],
        ]
    );

    // TiDB does NOT require the auto column to be a key -- captured, and
    // unlike MySQL, which raises 1075 for it.
    crate::run_create_table_on(
        "CREATE TABLE bad (a BIGINT AUTO_INCREMENT, b BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO bad (b) VALUES (1), (2)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a FROM bad",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );

    // A second auto column is Go's 1075, and a non-integer one is its
    // "Incorrect column specifier" -- both captured from TiDB.
    assert!(matches!(
        crate::run_create_table_on(
            "CREATE TABLE two (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT AUTO_INCREMENT)",
            &mut catalog
        ),
        Err(DriverError::WrongAutoKey)
    ));
    assert!(matches!(
        crate::run_create_table_on(
            "CREATE TABLE strk (a VARCHAR(4) AUTO_INCREMENT PRIMARY KEY)",
            &mut catalog
        ),
        Err(DriverError::WrongColumnSpecifier(_))
    ));
}
