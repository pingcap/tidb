//! The values a column supplies when a statement does not: `DEFAULT`, `NOT
//! NULL` rejection, and `AUTO_INCREMENT`.
//!
//! Mirrors Go `pkg/table`'s `GetColDefaultValue`/`GetColOriginDefaultValue`
//! and `pkg/meta/autoid`'s allocation, including the ids a rollback does NOT
//! hand back.

use super::*;

/// A version-1+ literal TIMESTAMP default is stored as a UTC wall clock.
/// Explicit INSERT and UPDATE DEFAULT must materialize that same default in
/// the writing statement's zone before the row codec converts it back to its
/// stored instant; treating DEFAULT as a generic expression never reaches
/// this conversion at all.
#[test]
fn explicit_timestamp_default_uses_the_writing_session_zone() {
    let store = crate::test_store::store();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    let utc = tidb_datatype::SessionTimeZone::utc();
    let plus_eight = tidb_datatype::SessionTimeZone::Fixed {
        name: "+08:00".to_owned(),
        offset_secs: 8 * 60 * 60,
    };
    let create_ctx = crate::StmtContext::default().with_txn_store(Box::new(store.clone()))
        .with_strict(true)
        .with_clock((1_600_000_000, 0, 0), utc.clone());
    crate::ddl::run_create_table_on_with(
        "CREATE TABLE explicit_ts (id BIGINT PRIMARY KEY, \
         ts TIMESTAMP DEFAULT '2020-01-02 00:00:00')",
        &mut catalog,
        "test",
        &create_ctx,
    )
    .unwrap();

    let write_ctx = crate::StmtContext::for_dml(false, true, false).with_txn_store(Box::new(store.clone()))
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
        let read_ctx = crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())).with_clock((1_600_000_000, 0, 0), zone);
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
    let store = crate::test_store::store();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE named_default (id BIGINT PRIMARY KEY, a BIGINT DEFAULT 1, \
         b BIGINT DEFAULT 2, c BIGINT DEFAULT 3)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_dml(false, true, false).with_txn_store(Box::new(store.clone()));
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
            &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
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
            &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
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
    let store = crate::test_store::store();
    use std::sync::Arc;

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE default_rng (a DOUBLE DEFAULT (rand()), b DOUBLE)",
        &mut catalog,
    )
    .unwrap();
    let actual_rng = Arc::new(tidb_mathutil::MysqlRng::new_with_seed(1));
    let ctx =
        crate::StmtContext::for_dml(false, true, false).with_txn_store(Box::new(store.clone())).with_rand_session(Arc::clone(&actual_rng));
    run_insert_on(
        "INSERT INTO default_rng VALUES (DEFAULT, RAND()), (DEFAULT, RAND())",
        &mut catalog,
        &ctx,
    )
    .unwrap();

    let expected_rng = tidb_mathutil::MysqlRng::new_with_seed(1);
    let first_default = expected_rng.gen();
    let second_default = expected_rng.gen();
    let first_runtime = expected_rng.gen();
    let second_runtime = expected_rng.gen();
    assert_eq!(
        run_select_on(
            "SELECT a, b FROM default_rng",
            &catalog,
            &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
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
    let store = crate::test_store::store();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE generated_default (id BIGINT PRIMARY KEY, a BIGINT DEFAULT 5, \
         g BIGINT GENERATED ALWAYS AS (a + 1) VIRTUAL)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_dml(false, true, false).with_txn_store(Box::new(store.clone()));
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
            (run_insert_on(sql, &mut catalog, &ctx)).is_err_and(
                |error| error.equal(&tidb_plannererrors::ERR_BAD_GENERATED_COLUMN)
            ),
            "{sql}"
        );
    }
    assert!((run_update_on(
        "UPDATE generated_default SET g = DEFAULT(a) WHERE id = 1",
        &mut catalog,
        &ctx,
    ))
    .is_err_and(|error| error.equal(&tidb_plannererrors::ERR_BAD_GENERATED_COLUMN)));
    assert!(run_select_on(
        "SELECT id FROM generated_default WHERE id = 3",
        &catalog,
        &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
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
            &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
        )
        .unwrap(),
        vec![vec![Datum::Null, Datum::Null]]
    );
}

/// ON DUPLICATE defaults are resolved before conflict detection and reused
/// as typed constants when a conflict does occur.
#[test]
fn on_duplicate_defaults_cover_conflict_and_no_conflict() {
    let store = crate::test_store::store();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE duplicate_default (id BIGINT PRIMARY KEY, a BIGINT DEFAULT 7, \
         b BIGINT DEFAULT 9)",
        &mut catalog,
    )
    .unwrap();
    let ctx = crate::StmtContext::for_dml(false, true, false).with_txn_store(Box::new(store.clone()));
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
            &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
        )
        .unwrap(),
        vec![vec![Datum::Int(9), Datum::Int(9)]]
    );
    assert!(run_insert_on(
        "INSERT INTO duplicate_default VALUES (2, 1, 2) \
         ON DUPLICATE KEY UPDATE a = DEFAULT(no_such_column)",
        &mut catalog,
        &ctx,
    )
    .is_err_and(|error| error.equal(&tidb_plannererrors::ERR_UNKNOWN_COLUMN)));
    assert!(run_select_on(
        "SELECT id FROM duplicate_default WHERE id = 2",
        &catalog,
        &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
    )
    .unwrap()
    .is_empty());
}

/// `CheckNoDefaultValueForInsert` is exclusive to INSERT defaults, while the
/// shared nil-default materializer still gives NOT NULL ENUM its first member
/// and evaluates UPDATE defaults even when no row can match.
#[test]
fn explicit_no_default_and_not_null_enum_follow_go_order() {
    let store = crate::test_store::store();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE nil_default (id BIGINT PRIMARY KEY, n BIGINT NOT NULL, \
         e ENUM('first','second') NOT NULL)",
        &mut catalog,
    )
    .unwrap();
    let strict = crate::StmtContext::for_dml(false, true, false).with_txn_store(Box::new(store.clone()));
    // The planner builds the value (Go `getDefaultValueForInsert`), so the
    // refusal reaches the client as ErrNoDefaultValue (1364).
    let error = run_insert_on(
        "INSERT INTO nil_default (id, n, e) VALUES (1, DEFAULT, DEFAULT)",
        &mut catalog,
        &strict,
    )
    .unwrap_err()
    .to_mysql_error();
    assert_eq!(error.code, 1364);
    assert_eq!(error.message, "Field 'n' doesn't have a default value");

    let lenient = crate::StmtContext::for_dml(false, false, false).with_txn_store(Box::new(store.clone()));
    run_insert_on(
        "INSERT INTO nil_default (id, n, e) VALUES (1, DEFAULT, DEFAULT)",
        &mut catalog,
        &lenient,
    )
    .unwrap();
    let warnings = lenient.take_warnings();
    assert_eq!(
        warnings.iter().filter(|warning| warning.code() == 1364).count(),
        1
    );
    let row = &run_select_on(
        "SELECT n, e FROM nil_default",
        &catalog,
        &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
    )
    .unwrap()[0];
    assert_eq!(row[0], Datum::Int(0));
    assert_eq!(row[1].go_bytes(), b"first");

    // The planner builds the assignment (Go `buildUpdateLists`, or the fast
    // plan's `buildOrderedList`), so the refusal reaches the client as
    // ErrNoDefaultValue (1364) whichever read the statement has.
    for statement in [
        "UPDATE nil_default SET n = DEFAULT WHERE id = 999",
        "UPDATE nil_default SET n = DEFAULT WHERE id > 998",
    ] {
        let error = run_update_on(statement, &mut catalog, &strict)
            .unwrap_err()
            .to_mysql_error();
        assert_eq!(error.code, 1364, "{statement}");
        assert_eq!(error.message, "Field 'n' doesn't have a default value");
    }
}

/// Multi-table UPDATE resolves each DEFAULT through the joined scope, but
/// materializes the referenced base column's metadata once before row work.
#[test]
fn multi_update_defaults_resolve_across_sources() {
    let store = crate::test_store::store();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
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
    let ctx = || crate::StmtContext::for_dml(false, true, false).with_txn_store(Box::new(store.clone()));
    run_insert_on(
        "INSERT INTO default_left VALUES (1, 100)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO default_right VALUES (1, 200)",
        &mut catalog,
        &ctx(),
    )
    .unwrap();
    assert_eq!(
        run_update_on(
            "UPDATE default_left l JOIN default_right r ON l.id = r.id \
             SET l.v = DEFAULT(r.v), r.v = DEFAULT(l.v)",
            &mut catalog,
            &ctx(),
        )
        .unwrap(),
        2
    );
    assert_eq!(
        run_select_on(
            "SELECT l.v, r.v FROM default_left l JOIN default_right r ON l.id = r.id",
            &catalog,
            &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
        )
        .unwrap(),
        vec![vec![Datum::Int(22), Datum::Int(11)]]
    );
}

/// Target/generated validation precedes opening an INSERT SELECT source, so a
/// rejected statement cannot consume the source expression's side effect.
#[test]
fn insert_select_generated_target_is_rejected_before_source_effects() {
    let store = crate::test_store::store();
    use tidb_variable::user_vars::{UserVars, UserVarsReader};

    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE select_default (a BIGINT, g BIGINT GENERATED ALWAYS AS (a + 1) VIRTUAL)",
        &mut catalog,
    )
    .unwrap();
    let variables = UserVars::new();
    variables.set_user_var_val("probe", Datum::Int(0));
    let ctx = crate::StmtContext::for_dml(false, true, false).with_txn_store(Box::new(store.clone())).with_user_vars(variables.clone());
    assert!((run_insert_on(
        "INSERT INTO select_default (g) SELECT @probe := 1",
        &mut catalog,
        &ctx,
    ))
    .is_err_and(|error| error.equal(&tidb_plannererrors::ERR_BAD_GENERATED_COLUMN)));
    assert_eq!(variables.get_user_var_val("probe"), Some(Datum::Int(0)));
}

/// AUTO_INCREMENT, checked against behavior captured from real TiDB:
/// inserting 1,2 then an explicit 100 rebases the allocator, so the next
/// rows are 101, 102, 103 -- NULL and 0 both allocate.
#[test]
fn auto_increment() {
    let store = crate::test_store::store();
    let mut catalog = tidb_infoschema_perfschema::mock_catalog();
    crate::run_create_table_on(
        "CREATE TABLE a1 (id BIGINT AUTO_INCREMENT PRIMARY KEY, v BIGINT)",
        &mut catalog,
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 (v) VALUES (10), (20)",
        &mut catalog,
        &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 VALUES (100, 30)",
        &mut catalog,
        &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 (v) VALUES (40)",
        &mut catalog,
        &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
    )
    .unwrap();
    run_insert_on(
        "INSERT INTO a1 VALUES (NULL, 50), (0, 60)",
        &mut catalog,
        &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
    )
    .unwrap();

    // Captured from TiDB: [[1 10] [2 20] [100 30] [101 40] [102 50] [103 60]]
    assert_eq!(
        run_select_on(
            "SELECT id, v FROM a1",
            &catalog,
            &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()))
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
        &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone())),
    )
    .unwrap();
    assert_eq!(
        run_select_on(
            "SELECT a FROM bad",
            &catalog,
            &crate::StmtContext::for_query().with_txn_store(Box::new(store.clone()))
        )
        .unwrap(),
        vec![vec![Datum::Int(1)], vec![Datum::Int(2)]]
    );

    // A second auto column is Go's 1075, and a non-integer one is its
    // "Incorrect column specifier" -- both captured from TiDB.
    assert_eq!(
        crate::run_create_table_on(
            "CREATE TABLE two (a BIGINT AUTO_INCREMENT PRIMARY KEY, b BIGINT AUTO_INCREMENT)",
            &mut catalog
        )
        .unwrap_err()
        .to_mysql_error()
        .code,
        1075
    );
    // Go's preprocessor raises it with a plain `errors.Errorf` (1105).
    let error = crate::run_create_table_on(
        "CREATE TABLE strk (a VARCHAR(4) AUTO_INCREMENT PRIMARY KEY)",
        &mut catalog,
    )
    .unwrap_err()
    .to_mysql_error();
    assert_eq!(
        (error.code, error.message.as_str()),
        (1105, "Incorrect column specifier for column 'a'")
    );
}
