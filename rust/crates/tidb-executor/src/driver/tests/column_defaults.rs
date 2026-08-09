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
    // A generated column carries no DEFAULT of its own: its value source is
    // the expression, which is what the row reads back (see
    // `crate::generated_column`).
    crate::run_create_table_on(
        "CREATE TABLE g2 (a BIGINT, b BIGINT GENERATED ALWAYS AS (a+1) VIRTUAL)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO g2 (a) VALUES (5)",
        &mut catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM g2",
            &catalog,
            &crate::StmtContext::for_query()
        )
        .unwrap(),
        vec![vec![Datum::Int(5), Datum::Int(6)]]
    );
}

/// Go resolves a bare assignment `DEFAULT` through the target column's
/// `GetColDefaultValue`; it is not a scalar expression for the generic
/// rewriter. Generated columns keep their one permitted explicit spelling:
/// `DEFAULT` means the generation expression remains their value source.
#[test]
fn explicit_default_uses_the_column_value_source() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE explicit_default (id BIGINT PRIMARY KEY, v BIGINT DEFAULT 7, \
         n BIGINT DEFAULT NULL, g BIGINT GENERATED ALWAYS AS (v + 1) VIRTUAL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_dml(false, true, false);

    run_insert_on(
        "INSERT INTO explicit_default (id, v, n, g) VALUES (1, DEFAULT, DEFAULT, DEFAULT)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT id, v, n, g FROM explicit_default",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![vec![
            Datum::Int(1),
            Datum::Int(7),
            Datum::Null,
            Datum::Int(8)
        ]]
    );

    run_update_on(
        "UPDATE explicit_default SET v = 20, g = DEFAULT WHERE id = 1",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT v, g FROM explicit_default",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![vec![Datum::Int(20), Datum::Int(21)]]
    );
    run_update_on(
        "UPDATE explicit_default SET v = DEFAULT, n = DEFAULT, g = DEFAULT WHERE id = 1",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT id, v, n, g FROM explicit_default",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![vec![
            Datum::Int(1),
            Datum::Int(7),
            Datum::Null,
            Datum::Int(8)
        ]]
    );
}

/// A version-1+ literal TIMESTAMP default is stored as a UTC wall clock.
/// Explicit INSERT and UPDATE DEFAULT must materialize that same default in
/// the writing statement's zone before the row codec converts it back to its
/// stored instant; treating DEFAULT as a generic expression never reaches
/// this conversion at all.
#[test]
fn explicit_timestamp_default_uses_the_writing_session_zone() {
    let mut catalog = Catalog::default();
    let utc = tidb_datatype::SessionTimeZone::utc();
    let plus_eight = tidb_datatype::SessionTimeZone::Fixed {
        name: "+08:00".to_owned(),
        offset_secs: 8 * 60 * 60,
    };
    let create_ctx = crate::StmtContext::default()
        .with_strict(true)
        .with_clock((1_600_000_000, 0, 0), utc.clone());
    crate::ddl::run_create_table_in(
        "CREATE TABLE explicit_ts (id BIGINT PRIMARY KEY, \
         ts TIMESTAMP DEFAULT '2020-01-02 00:00:00')",
        &mut catalog,
        "test",
        crate::CreateTableSettings::default(),
        &create_ctx,
    )
    .unwrap();

    let write_ctx = crate::StmtContext::for_dml(false, true, false)
        .with_clock((1_600_000_000, 0, 0), plus_eight.clone());
    run_insert_on(
        "INSERT INTO explicit_ts (id, ts) VALUES \
         (1, DEFAULT), (2, '2020-03-04 12:00:00')",
        &mut catalog,
        &write_ctx,
    )
    .unwrap();
    run_update_on(
        "UPDATE explicit_ts SET ts = DEFAULT WHERE id = 2",
        &mut catalog,
        &write_ctx,
    )
    .unwrap();

    for (zone, expected) in [
        (plus_eight, "2020-01-02 08:00:00"),
        (utc, "2020-01-02 00:00:00"),
    ] {
        let read_ctx = crate::StmtContext::for_query().with_clock((1_600_000_000, 0, 0), zone);
        let rows = run_select_on(
            "SELECT ts FROM explicit_ts ORDER BY id",
            &catalog,
            &read_ctx,
        )
        .unwrap();
        assert_eq!(rows.len(), 2);
        for row in rows {
            let Datum::Time(time) = &row[0] else {
                panic!("expected a TIMESTAMP, got {:?}", row[0]);
            };
            assert_eq!(time.to_string(), expected);
        }
    }
}

/// Accepted Go issue-7061 shape: the column named inside DEFAULT is the value
/// source, independently of the INSERT target, and named defaults remain
/// ordinary leaves when nested in a larger scalar expression.
#[test]
fn named_and_nested_defaults_use_the_referenced_column() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE named_default (id BIGINT PRIMARY KEY, a BIGINT DEFAULT 1, \
         b BIGINT DEFAULT 2, c BIGINT DEFAULT 3)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_dml(false, true, false);
    run_insert_on(
        "INSERT INTO named_default (id, a, b, c) VALUES \
         (1, DEFAULT(b), DEFAULT(a) + DEFAULT(b), DEFAULT)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO named_default (id, b) VALUES (2, DEFAULT(a))",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT id, a, b, c FROM named_default ORDER BY id",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![
            vec![Datum::Int(1), Datum::Int(2), Datum::Int(3), Datum::Int(3)],
            vec![Datum::Int(2), Datum::Int(1), Datum::Int(1), Datum::Int(3)],
        ]
    );

    run_update_on(
        "UPDATE named_default SET a = DEFAULT(b), b = DEFAULT(a), \
         c = DEFAULT(a) + DEFAULT(b) WHERE id = 1",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a, b, c FROM named_default WHERE id = 1",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![vec![Datum::Int(2), Datum::Int(1), Datum::Int(3)]]
    );
}

/// Explicit VALUES defaults are plan constants. All of them consume the
/// session RNG in row-major order before runtime RAND() expressions consume
/// anything; treating DEFAULT as omission interleaves the two sequences.
#[test]
fn explicit_values_defaults_are_lowered_before_runtime_expressions() {
    use std::cell::RefCell;
    use std::rc::Rc;

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE default_rng (a DOUBLE DEFAULT (rand()), b DOUBLE)",
        &mut catalog,
    )
    .unwrap();
    let actual_rng = Rc::new(RefCell::new(tidb_expr::MysqlRng::new_with_seed(1)));
    let ctx =
        crate::StmtContext::for_dml(false, true, false).with_rand_session(Rc::clone(&actual_rng));
    run_insert_on(
        "INSERT INTO default_rng VALUES (DEFAULT, RAND()), (DEFAULT, RAND())",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let mut expected_rng = tidb_expr::MysqlRng::new_with_seed(1);
    let first_default = expected_rng.gen();
    let second_default = expected_rng.gen();
    let first_runtime = expected_rng.gen();
    let second_runtime = expected_rng.gen();
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM default_rng",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![
            vec![Datum::Real(first_default), Datum::Real(first_runtime)],
            vec![Datum::Real(second_default), Datum::Real(second_runtime)],
        ]
    );
}

/// Generated targets accept exactly bare DEFAULT or DEFAULT(the same
/// generated column). Cross-column DEFAULT keeps error 3105 across INSERT,
/// UPDATE and ON DUPLICATE, including an ON DUPLICATE statement that would
/// not encounter a conflict.
#[test]
fn generated_default_identity_matrix_and_on_duplicate_timing() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE generated_default (id BIGINT PRIMARY KEY, a BIGINT DEFAULT 5, \
         g BIGINT GENERATED ALWAYS AS (a + 1) VIRTUAL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_dml(false, true, false);
    run_insert_on(
        "INSERT INTO generated_default VALUES (1, DEFAULT, DEFAULT), \
         (2, DEFAULT, DEFAULT(g))",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    for sql in [
        "INSERT INTO generated_default (id, g) VALUES (3, DEFAULT(a))",
        "INSERT INTO generated_default (id, a) VALUES (3, DEFAULT(g))",
        "INSERT INTO generated_default (id, a) VALUES (3, 8) \
         ON DUPLICATE KEY UPDATE g = DEFAULT(a)",
    ] {
        assert!(
            matches!(
                run_insert_on(sql, &mut catalog, &ctx),
                Err(DriverError::BadGeneratedColumn { .. })
            ),
            "{sql}"
        );
    }
    assert!(matches!(
        run_update_on(
            "UPDATE generated_default SET g = DEFAULT(a) WHERE id = 1",
            &mut catalog,
            &ctx,
        ),
        Err(DriverError::BadGeneratedColumn { .. })
    ));
    assert!(run_select_on(
        "SELECT id FROM generated_default WHERE id = 3",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap()
    .is_empty());

    run_update_on(
        "UPDATE generated_default SET g = DEFAULT(g) WHERE id = 1",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    // A normal target may read DEFAULT(generated) in UPDATE; Go's default
    // evaluator returns NULL for this nullable generated column.
    run_update_on(
        "UPDATE generated_default SET a = DEFAULT(g) WHERE id = 2",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a, g FROM generated_default WHERE id = 2",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![vec![Datum::Null, Datum::Null]]
    );
}

/// ON DUPLICATE defaults are resolved before conflict detection and reused
/// as typed constants when a conflict does occur.
#[test]
fn on_duplicate_defaults_cover_conflict_and_no_conflict() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE duplicate_default (id BIGINT PRIMARY KEY, a BIGINT DEFAULT 7, \
         b BIGINT DEFAULT 9)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_dml(false, true, false);
    run_insert_on(
        "INSERT INTO duplicate_default VALUES (1, 0, 0)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO duplicate_default VALUES (1, 1, 2) \
         ON DUPLICATE KEY UPDATE a = DEFAULT(b), b = DEFAULT(a) + VALUES(b)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM duplicate_default WHERE id = 1",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![vec![Datum::Int(9), Datum::Int(9)]]
    );
    assert!(matches!(
        run_insert_on(
            "INSERT INTO duplicate_default VALUES (2, 1, 2) \
             ON DUPLICATE KEY UPDATE a = DEFAULT(no_such_column)",
            &mut catalog,
            &ctx,
        ),
        Err(DriverError::UnknownColumnInClause { .. })
    ));
    assert!(run_select_on(
        "SELECT id FROM duplicate_default WHERE id = 2",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap()
    .is_empty());
}

/// `CheckNoDefaultValueForInsert` is exclusive to INSERT defaults, while the
/// shared nil-default materializer still gives NOT NULL ENUM its first member
/// and evaluates UPDATE defaults even when no row can match.
#[test]
fn explicit_no_default_and_not_null_enum_follow_go_order() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE nil_default (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, \
         e ENUM('first','second') NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    let strict = crate::StmtContext::for_dml(false, true, false);
    assert!(matches!(
        run_insert_on(
            "INSERT INTO nil_default (id, n, e) VALUES (1, DEFAULT, DEFAULT)",
            &mut catalog,
            &strict,
        ),
        Err(DriverError::NoDefaultForField(name)) if name == "n"
    ));

    let lenient = crate::StmtContext::for_dml(false, false, false);
    run_insert_on(
        "INSERT INTO nil_default (id, n, e) VALUES (1, DEFAULT, DEFAULT)",
        &mut catalog,
        &lenient,
    )
    .unwrap();
    let warnings = lenient.take_warnings();
    assert_eq!(
        warnings.iter().filter(|warning| warning.1 == 1364).count(),
        1
    );
    let row = &run_select_on(
        "SELECT n, e FROM nil_default",
        &catalog,
        &crate::StmtContext::for_query(),
    )
    .unwrap()[0];
    assert_eq!(row[0], Datum::Int(0));
    assert_eq!(row[1].go_bytes(), b"first");

    assert!(matches!(
        run_update_on(
            "UPDATE nil_default SET n = DEFAULT WHERE id = 999",
            &mut catalog,
            &strict,
        ),
        Err(DriverError::NoDefaultForField(name)) if name == "n"
    ));
}

/// Multi-table UPDATE resolves each DEFAULT through the joined scope, but
/// materializes the referenced base column's metadata once before row work.
#[test]
fn multi_update_defaults_resolve_across_sources() {
    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE default_left (id BIGINT PRIMARY KEY, v BIGINT DEFAULT 11)",
        &mut catalog,
    )
    .unwrap();
    crate::run_create_table_on(
        "CREATE TABLE default_right (id BIGINT PRIMARY KEY, v BIGINT DEFAULT 22)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_dml(false, true, false);
    run_insert_on(
        "INSERT INTO default_left VALUES (1, 100)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO default_right VALUES (1, 200)",
        &mut catalog,
        &ctx,
    )
    .unwrap();
    assert_eq!(
        run_update_on(
            "UPDATE default_left l JOIN default_right r ON l.id = r.id \
             SET l.v = DEFAULT(r.v), r.v = DEFAULT(l.v)",
            &mut catalog,
            &ctx,
        )
        .unwrap(),
        2
    );
    assert_eq!(
        run_select_on(
            "SELECT l.v, r.v FROM default_left l JOIN default_right r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query(),
        )
        .unwrap(),
        vec![vec![Datum::Int(22), Datum::Int(11)]]
    );
}

/// Target/generated validation precedes opening an INSERT SELECT source, so a
/// rejected statement cannot consume the source expression's side effect.
#[test]
fn insert_select_generated_target_is_rejected_before_source_effects() {
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    let mut catalog = Catalog::default();
    crate::run_create_table_on(
        "CREATE TABLE select_default (a BIGINT, g BIGINT GENERATED ALWAYS AS (a + 1) VIRTUAL)",
        &mut catalog,
    )
    .unwrap();
    let variables = Rc::new(RefCell::new(HashMap::from([(
        "probe".to_owned(),
        Datum::Int(0),
    )])));
    let ctx = crate::StmtContext::for_dml(false, true, false).with_user_vars(Rc::clone(&variables));
    assert!(matches!(
        run_insert_on(
            "INSERT INTO select_default (g) SELECT @probe := 1",
            &mut catalog,
            &ctx,
        ),
        Err(DriverError::BadGeneratedColumn { .. })
    ));
    assert_eq!(variables.borrow().get("probe"), Some(&Datum::Int(0)));
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
